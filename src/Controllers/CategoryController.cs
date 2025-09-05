using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class CategoryController : ControllerBase
{
    private readonly IRepository<Category> _categoryRepo;
    private readonly IRepository<AuthorCategory> _authorCategoryRepo;

    public CategoryController(IRepository<Category> categoryRepo, IRepository<AuthorCategory> authorCategoryRepo)
    {
        _categoryRepo = categoryRepo;
        _authorCategoryRepo = authorCategoryRepo;
    }

    [HttpGet]
    public async Task<IActionResult> GetCategories([FromQuery] bool includeAuthors = false)
    {
        try
        {
            var query = _categoryRepo.Query();
            
            if (includeAuthors)
                query = query.Include(c => c.Authors);

            var categories = query.Execute().ToList();
            return Ok(categories);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve categories", details = ex.Message });
        }
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetCategory(int id, [FromQuery] bool includeAuthors = false)
    {
        if (id <= 0)
            return BadRequest("Invalid category ID");

        try
        {
            var query = _categoryRepo.Query().Where(c => c.Id == id);
            
            if (includeAuthors)
                query = query.Include(c => c.Authors);

            var category = query.Execute().FirstOrDefault();
            if (category == null)
                return NotFound($"Category with ID {id} not found");
            return Ok(category);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve category", details = ex.Message });
        }
    }

    [HttpGet("where")]
    public async Task<IActionResult> GetCategoriesWhere([FromQuery] string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return BadRequest("Name parameter is required and cannot be empty");

        try
        {
            var categories = await _categoryRepo.ReadManyAsync(c => c.Name.Contains(name)).ToListAsync();
            return Ok(categories);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to search categories", details = ex.Message });
        }
    }

    [HttpGet("paged")]
    public async Task<IActionResult> GetCategoriesPaged([FromQuery] int skip = 0, [FromQuery] int take = 10)
    {
        if (skip < 0)
            return BadRequest("Skip parameter must be non-negative");
        if (take < 1 || take > 100)
            return BadRequest("Take parameter must be between 1 and 100");

        try
        {
            var allCategories = await _categoryRepo.ReadAllAsync()
                .OrderBy(c => c.Name)
                .Skip(skip)
                .Take(take)
                .ToListAsync();
            return Ok(allCategories);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve paged categories", details = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> CreateCategory([FromBody] Category category)
    {
        if (category == null)
            return BadRequest("Category data is required");

        if (!ModelState.IsValid)
        {
            var errors = ModelState
                .Where(x => x.Value.Errors.Count > 0)
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value.Errors.Select(e => e.ErrorMessage).ToArray()
                );

            return BadRequest(new
            {
                error = "Validation failed",
                details = "One or more validation errors occurred",
                validationErrors = errors
            });
        }

        try
        {
            var created = await _categoryRepo.CreateAsync(category);
            return Ok(created);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to create category", details = ex.Message });
        }
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> UpdateCategory(int id, [FromBody] Category category)
    {
        if (id <= 0)
            return BadRequest("Invalid category ID");
        if (category == null)
            return BadRequest("Category data is required");

        if (!ModelState.IsValid)
        {
            var errors = ModelState
                .Where(x => x.Value.Errors.Count > 0)
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value.Errors.Select(e => e.ErrorMessage).ToArray()
                );

            return BadRequest(new
            {
                error = "Validation failed",
                details = "One or more validation errors occurred",
                validationErrors = errors
            });
        }

        try
        {
            // Check if category exists
            var existing = await _categoryRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Category with ID {id} not found");

            category.Id = id;
            var updated = await _categoryRepo.UpdateAsync(category);
            return Ok(updated);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to update category", details = ex.Message });
        }
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> DeleteCategory(int id)
    {
        if (id <= 0)
            return BadRequest("Invalid category ID");

        try
        {
            // Check if category exists
            var existing = await _categoryRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Category with ID {id} not found");

            await _categoryRepo.DeleteByIdAsync(id);
            return NoContent();
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19) // FOREIGN KEY constraint
        {
            return BadRequest(new { error = "Cannot delete category", details = "Category is referenced by author relationships" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to delete category", details = ex.Message });
        }
    }

    [HttpGet("test-include/{id}")]
    public async Task<IActionResult> TestCategoryInclude(int id)
    {
        try
        {
            // Test different ways to load the many-to-many relationship
            var results = new
            {
                // Method 1: Using Include
                withInclude = _categoryRepo.Query()
                    .Where(c => c.Id == id)
                    .Include(c => c.Authors)
                    .Execute()
                    .FirstOrDefault(),

                // Method 2: Manual join through junction table
                manualJoin = await GetCategoryWithAuthorsManually(id),

                // Method 3: Check raw junction table data
                junctionData = await _authorCategoryRepo.ReadManyAsync(ac => ac.CategoryId == id).ToListAsync()
            };

            return Ok(results);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed in test", details = ex.Message, stackTrace = ex.StackTrace });
        }
    }

    private async Task<object> GetCategoryWithAuthorsManually(int categoryId)
    {
        var category = await _categoryRepo.ReadByIdAsync(categoryId);
        if (category == null) return null;

        // Get author IDs from junction table
        var authorCategories = await _authorCategoryRepo.ReadManyAsync(ac => ac.CategoryId == categoryId).ToListAsync();
        
        // Load each author
        var authors = new List<Author>();
        foreach (var ac in authorCategories)
        {
            // We'd need the author repository here, but for now just show the IDs
            authors.Add(new Author { Id = ac.AuthorId, Name = $"Author ID: {ac.AuthorId}" });
        }

        return new
        {
            category = category,
            authorCount = authors.Count,
            authorIds = authorCategories.Select(ac => ac.AuthorId).ToList(),
            authors = authors
        };
    }
}