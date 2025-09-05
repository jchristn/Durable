using Microsoft.AspNetCore.Mvc;
using System.Linq.Expressions;
using Microsoft.Data.Sqlite;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class QueryTestController : ControllerBase
{
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;

    private readonly IRepository<Company> _companyRepo;
    private readonly IRepository<Category> _categoryRepo;
    private readonly IRepository<AuthorCategory> _authorCategoryRepo;

    public QueryTestController(IRepository<Author> authorRepo, IRepository<Book> bookRepo, IRepository<Company> companyRepo, IRepository<Category> categoryRepo, IRepository<AuthorCategory> authorCategoryRepo)
    {
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
        _companyRepo = companyRepo;
        _categoryRepo = categoryRepo;
        _authorCategoryRepo = authorCategoryRepo;
    }

    [HttpGet("authors")]
    public async Task<IActionResult> GetAuthors([FromQuery] bool includeCompany = false, [FromQuery] bool includeBooks = false, [FromQuery] bool includeCategories = false)
    {
        try
        {
            var query = _authorRepo.Query();
            
            if (includeCompany)
                query = query.Include(a => a.Company);
            
            if (includeBooks)
                query = query.Include(a => a.Books);
            
            if (includeCategories)
                query = query.Include(a => a.Categories);

            var authors = query.Execute().ToList();
            return Ok(authors);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve authors", details = ex.Message });
        }
    }

    [HttpGet("authors/where")]
    public async Task<IActionResult> GetAuthorsWhere([FromQuery] string name, [FromQuery] bool includeCompany = false, [FromQuery] bool includeBooks = false, [FromQuery] bool includeCategories = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            return BadRequest("Name parameter is required and cannot be empty");

        try
        {
            var query = _authorRepo.Query().Where(a => a.Name.Contains(name));
            
            if (includeCompany)
                query = query.Include(a => a.Company);
            
            if (includeBooks)
                query = query.Include(a => a.Books);
            
            if (includeCategories)
                query = query.Include(a => a.Categories);

            var authors = query.Execute().ToList();
            return Ok(authors);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to search authors", details = ex.Message });
        }
    }

    [HttpGet("authors/paged")]
    public async Task<IActionResult> GetAuthorsPaged([FromQuery] int skip = 0, [FromQuery] int take = 10)
    {
        if (skip < 0)
            return BadRequest("Skip parameter must be non-negative");
        if (take < 1 || take > 100)
            return BadRequest("Take parameter must be between 1 and 100");

        try
        {
            var allAuthors = await _authorRepo.ReadAllAsync()
                .OrderBy(a => a.Name)
                .Skip(skip)
                .Take(take)
                .ToListAsync();
            return Ok(allAuthors);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve paged authors", details = ex.Message });
        }
    }

    [HttpGet("authors/projection")]
    public async Task<IActionResult> GetAuthorNames()
    {
        try
        {
            var authors = await _authorRepo.ReadAllAsync().ToListAsync();
            var names = authors.Select(a => new { a.Id, a.Name });
            return Ok(names);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve author names", details = ex.Message });
        }
    }

    [HttpGet("books/complex-query")]
    public async Task<IActionResult> ComplexBookQuery(
        [FromQuery] string? title,
        [FromQuery] bool includeAuthor = false,
        [FromQuery] bool includePublisher = false,
        [FromQuery] bool includeAuthorCompany = false)
    {
        try
        {
            var query = _bookRepo.Query();
            
            if (!string.IsNullOrEmpty(title))
                query = query.Where(b => b.Title.Contains(title));
            
            if (includeAuthor)
            {
                query = query.Include(b => b.Author);
                
                if (includeAuthorCompany)
                    query = query.ThenInclude<Author, Company>(a => a.Company);
            }
            
            if (includePublisher)
                query = query.Include(b => b.Publisher);

            var books = query.Execute().ToList();
                
            return Ok(books);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to query books", details = ex.Message });
        }
    }

    private async Task<(bool IsValid, string ErrorMessage)> ValidateForeignKeysAsync(Author author)
    {
        if (author.CompanyId.HasValue)
        {
            var companyExists = await _companyRepo.ExistsAsync(c => c.Id == author.CompanyId.Value);
            if (!companyExists)
                return (false, $"Company with ID {author.CompanyId.Value} does not exist");
        }

        return (true, string.Empty);
    }

    [HttpPost("authors")]
    public async Task<IActionResult> CreateAuthor([FromBody] Author author)
    {
        if (author == null)
            return BadRequest("Author data is required");

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

        // Validate foreign key references
        var (isValid, errorMessage) = await ValidateForeignKeysAsync(author);
        if (!isValid)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = errorMessage });
        }

        try
        {
            var created = await _authorRepo.CreateAsync(author);
            return Ok(created);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19) // FOREIGN KEY constraint failed
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = "Referenced company or category does not exist" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to create author", details = ex.Message });
        }
    }

    [HttpPut("authors/{id}")]
    public async Task<IActionResult> UpdateAuthor(int id, [FromBody] Author author)
    {
        if (id <= 0)
            return BadRequest("Invalid author ID");
        if (author == null)
            return BadRequest("Author data is required");

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

        // Validate foreign key references
        var (isValid, errorMessage) = await ValidateForeignKeysAsync(author);
        if (!isValid)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = errorMessage });
        }

        try
        {
            // Check if author exists
            var existing = await _authorRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Author with ID {id} not found");

            author.Id = id;
            var updated = await _authorRepo.UpdateAsync(author);
            return Ok(updated);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = "Referenced company or category does not exist" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to update author", details = ex.Message });
        }
    }

    [HttpDelete("authors/{id}")]
    public async Task<IActionResult> DeleteAuthor(int id)
    {
        if (id <= 0)
            return BadRequest("Invalid author ID");

        try
        {
            // Check if author exists
            var existing = await _authorRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Author with ID {id} not found");

            await _authorRepo.DeleteByIdAsync(id);
            return NoContent();
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to delete author", details = ex.Message });
        }
    }

    [HttpGet("authors/{id}/with-includes")]
    public async Task<IActionResult> GetAuthorWithIncludes(int id, [FromQuery] bool includeCompany = false, [FromQuery] bool includeBooks = false, [FromQuery] bool includeCategories = false)
    {
        if (id <= 0)
            return BadRequest("Invalid author ID");

        try
        {
            var query = _authorRepo.Query().Where(a => a.Id == id);
            
            if (includeCompany)
                query = query.Include(a => a.Company);
            
            if (includeBooks)
                query = query.Include(a => a.Books);
            
            if (includeCategories)
                query = query.Include(a => a.Categories);

            var author = query.Execute().FirstOrDefault();
            if (author == null)
                return NotFound($"Author with ID {id} not found");

            return Ok(author);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve author with includes", details = ex.Message });
        }
    }

    [HttpGet("authors/with-nested-includes")]
    public async Task<IActionResult> GetAuthorsWithNestedIncludes([FromQuery] int limit = 10)
    {
        if (limit < 1 || limit > 100)
            return BadRequest("Limit must be between 1 and 100");

        try
        {
            var authors = _authorRepo.Query()
                .Include(a => a.Company)
                .ThenInclude<Company, List<Book>>(c => c.PublishedBooks)
                .Include(a => a.Books)
                .ThenInclude<Book, Company>(b => b.Publisher)
                .Include(a => a.Categories)
                .OrderBy(a => a.Name)
                .Take(limit)
                .Execute()
                .ToList();

            return Ok(authors);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve authors with nested includes", details = ex.Message });
        }
    }

    [HttpGet("books/with-full-details")]
    public async Task<IActionResult> GetBooksWithFullDetails([FromQuery] int limit = 10)
    {
        if (limit < 1 || limit > 100)
            return BadRequest("Limit must be between 1 and 100");

        try
        {
            var books = _bookRepo.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Include(b => b.Publisher)
                .OrderBy(b => b.Title)
                .Take(limit)
                .Execute()
                .ToList();

            return Ok(books);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve books with full details", details = ex.Message });
        }
    }

    [HttpGet("test-linq")]
    public async Task<IActionResult> TestLinqCapabilities()
    {
        try
        {
            var tests = new
            {
                SimpleWhere = await _authorRepo.ReadManyAsync(a => a.Id > 0).ToListAsync(),
                AllAuthors = await _authorRepo.ReadAllAsync().OrderBy(a => a.Name).ThenBy(a => a.Id).ToListAsync(),
                FirstOrDefault = await _authorRepo.ReadFirstOrDefaultAsync(a => a.Id == 1),
                Any = await _authorRepo.ExistsAsync(a => a.Name.StartsWith("J")),
                Count = await _authorRepo.CountAsync()
            };

            return Ok(tests);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute LINQ tests", details = ex.Message });
        }
    }

    // Many-to-Many Relationship Management
    [HttpPost("authors/{authorId}/categories/{categoryId}")]
    public async Task<IActionResult> AddAuthorCategory(int authorId, int categoryId)
    {
        if (authorId <= 0)
            return BadRequest("Invalid author ID");
        if (categoryId <= 0)
            return BadRequest("Invalid category ID");

        try
        {
            // Validate that author exists
            var authorExists = await _authorRepo.ExistsAsync(a => a.Id == authorId);
            if (!authorExists)
                return NotFound($"Author with ID {authorId} not found");

            // Validate that category exists
            var categoryExists = await _categoryRepo.ExistsAsync(c => c.Id == categoryId);
            if (!categoryExists)
                return NotFound($"Category with ID {categoryId} not found");

            // Check if relationship already exists
            var relationshipExists = await _authorCategoryRepo.ExistsAsync(ac => ac.AuthorId == authorId && ac.CategoryId == categoryId);
            if (relationshipExists)
                return BadRequest("Author is already associated with this category");

            // Create the relationship
            var authorCategory = new AuthorCategory
            {
                AuthorId = authorId,
                CategoryId = categoryId
            };

            var created = await _authorCategoryRepo.CreateAsync(authorCategory);
            return Ok(created);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to add author-category relationship", details = ex.Message });
        }
    }

    [HttpDelete("authors/{authorId}/categories/{categoryId}")]
    public async Task<IActionResult> RemoveAuthorCategory(int authorId, int categoryId)
    {
        if (authorId <= 0)
            return BadRequest("Invalid author ID");
        if (categoryId <= 0)
            return BadRequest("Invalid category ID");

        try
        {
            // Find the relationship
            var relationship = await _authorCategoryRepo.ReadFirstOrDefaultAsync(ac => ac.AuthorId == authorId && ac.CategoryId == categoryId);
            if (relationship == null)
                return NotFound("Author-category relationship not found");

            await _authorCategoryRepo.DeleteByIdAsync(relationship.Id);
            return NoContent();
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to remove author-category relationship", details = ex.Message });
        }
    }

    [HttpGet("authors/{authorId}/categories")]
    public async Task<IActionResult> GetAuthorCategories(int authorId)
    {
        if (authorId <= 0)
            return BadRequest("Invalid author ID");

        try
        {
            // Validate that author exists
            var authorExists = await _authorRepo.ExistsAsync(a => a.Id == authorId);
            if (!authorExists)
                return NotFound($"Author with ID {authorId} not found");

            var relationships = await _authorCategoryRepo.ReadManyAsync(ac => ac.AuthorId == authorId).ToListAsync();
            var categoryIds = relationships.Select(r => r.CategoryId).ToList();
            
            var categories = new List<Category>();
            foreach (var categoryId in categoryIds)
            {
                var category = await _categoryRepo.ReadByIdAsync(categoryId);
                if (category != null)
                    categories.Add(category);
            }

            return Ok(categories);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve author categories", details = ex.Message });
        }
    }

    [HttpPut("authors/{authorId}/categories")]
    public async Task<IActionResult> UpdateAuthorCategories(int authorId, [FromBody] List<int> categoryIds)
    {
        if (authorId <= 0)
            return BadRequest("Invalid author ID");
        if (categoryIds == null)
            return BadRequest("Category IDs are required");

        try
        {
            // Validate that author exists
            var authorExists = await _authorRepo.ExistsAsync(a => a.Id == authorId);
            if (!authorExists)
                return NotFound($"Author with ID {authorId} not found");

            // Validate all category IDs exist
            foreach (var categoryId in categoryIds.Distinct())
            {
                var categoryExists = await _categoryRepo.ExistsAsync(c => c.Id == categoryId);
                if (!categoryExists)
                    return BadRequest($"Category with ID {categoryId} does not exist");
            }

            // Get current relationships
            var currentRelationships = await _authorCategoryRepo.ReadManyAsync(ac => ac.AuthorId == authorId).ToListAsync();
            var currentCategoryIds = currentRelationships.Select(r => r.CategoryId).ToList();

            // Remove relationships that are no longer needed
            var toRemove = currentCategoryIds.Except(categoryIds).ToList();
            foreach (var categoryId in toRemove)
            {
                var relationship = currentRelationships.First(r => r.CategoryId == categoryId);
                await _authorCategoryRepo.DeleteByIdAsync(relationship.Id);
            }

            // Add new relationships
            var toAdd = categoryIds.Except(currentCategoryIds).ToList();
            foreach (var categoryId in toAdd)
            {
                var authorCategory = new AuthorCategory
                {
                    AuthorId = authorId,
                    CategoryId = categoryId
                };
                await _authorCategoryRepo.CreateAsync(authorCategory);
            }

            return Ok(new { message = "Author categories updated successfully" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to update author categories", details = ex.Message });
        }
    }
}