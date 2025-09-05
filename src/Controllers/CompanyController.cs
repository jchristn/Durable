using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class CompanyController : ControllerBase
{
    private readonly IRepository<Company> _companyRepo;
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;

    public CompanyController(IRepository<Company> companyRepo, IRepository<Author> authorRepo, IRepository<Book> bookRepo)
    {
        _companyRepo = companyRepo;
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
    }

    [HttpGet]
    public async Task<IActionResult> GetCompanies([FromQuery] bool includeEmployees = false, [FromQuery] bool includePublishedBooks = false)
    {
        try
        {
            var query = _companyRepo.Query();
            
            if (includeEmployees)
                query = query.Include(c => c.Employees);
            
            if (includePublishedBooks)
                query = query.Include(c => c.PublishedBooks);

            var companies = query.Execute().ToList();
            return Ok(companies);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve companies", details = ex.Message });
        }
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetCompany(int id, [FromQuery] bool includeEmployees = false, [FromQuery] bool includePublishedBooks = false)
    {
        if (id <= 0)
            return BadRequest("Invalid company ID");

        try
        {
            var query = _companyRepo.Query().Where(c => c.Id == id);
            
            if (includeEmployees)
                query = query.Include(c => c.Employees);
            
            if (includePublishedBooks)
                query = query.Include(c => c.PublishedBooks);

            var company = query.Execute().FirstOrDefault();
            if (company == null)
                return NotFound($"Company with ID {id} not found");
            return Ok(company);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve company", details = ex.Message });
        }
    }

    [HttpGet("where")]
    public async Task<IActionResult> GetCompaniesWhere([FromQuery] string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return BadRequest("Name parameter is required and cannot be empty");

        try
        {
            var companies = await _companyRepo.ReadManyAsync(c => c.Name.Contains(name)).ToListAsync();
            return Ok(companies);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to search companies", details = ex.Message });
        }
    }

    [HttpGet("paged")]
    public async Task<IActionResult> GetCompaniesPaged([FromQuery] int skip = 0, [FromQuery] int take = 10)
    {
        if (skip < 0)
            return BadRequest("Skip parameter must be non-negative");
        if (take < 1 || take > 100)
            return BadRequest("Take parameter must be between 1 and 100");

        try
        {
            var allCompanies = await _companyRepo.ReadAllAsync()
                .OrderBy(c => c.Name)
                .Skip(skip)
                .Take(take)
                .ToListAsync();
            return Ok(allCompanies);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve paged companies", details = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> CreateCompany([FromBody] Company company)
    {
        if (company == null)
            return BadRequest("Company data is required");

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
            var created = await _companyRepo.CreateAsync(company);
            return Ok(created);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to create company", details = ex.Message });
        }
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> UpdateCompany(int id, [FromBody] Company company)
    {
        if (id <= 0)
            return BadRequest("Invalid company ID");
        if (company == null)
            return BadRequest("Company data is required");

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
            // Check if company exists
            var existing = await _companyRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Company with ID {id} not found");

            company.Id = id;
            var updated = await _companyRepo.UpdateAsync(company);
            return Ok(updated);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to update company", details = ex.Message });
        }
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> DeleteCompany(int id, [FromQuery] bool force = false)
    {
        if (id <= 0)
            return BadRequest("Invalid company ID");

        try
        {
            // Check if company exists
            var existing = await _companyRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Company with ID {id} not found");

            // Check for dependencies
            var employeeCount = await _authorRepo.CountAsync(a => a.CompanyId == id);
            var publishedBooksCount = await _bookRepo.CountAsync(b => b.PublisherId == id);
            
            if ((employeeCount > 0 || publishedBooksCount > 0) && !force)
            {
                var dependencies = new List<string>();
                if (employeeCount > 0) dependencies.Add($"{employeeCount} author(s)");
                if (publishedBooksCount > 0) dependencies.Add($"{publishedBooksCount} book(s)");
                
                return BadRequest(new 
                { 
                    error = "Cannot delete company - has dependencies", 
                    details = $"Company is referenced by {string.Join(" and ", dependencies)}. Use force=true to delete and set references to null.",
                    dependencies = new { employeeCount, publishedBooksCount }
                });
            }

            // If force is true, update references to null before deleting
            if (force && (employeeCount > 0 || publishedBooksCount > 0))
            {
                // Update authors to remove company reference
                var employees = await _authorRepo.ReadManyAsync(a => a.CompanyId == id).ToListAsync();
                foreach (var author in employees)
                {
                    author.CompanyId = null;
                    await _authorRepo.UpdateAsync(author);
                }

                // Update books to remove publisher reference
                var publishedBooks = await _bookRepo.ReadManyAsync(b => b.PublisherId == id).ToListAsync();
                foreach (var book in publishedBooks)
                {
                    book.PublisherId = null;
                    await _bookRepo.UpdateAsync(book);
                }
            }

            await _companyRepo.DeleteByIdAsync(id);
            return NoContent();
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19) // FOREIGN KEY constraint
        {
            return BadRequest(new { error = "Cannot delete company", details = "Company is referenced by other records. Database constraints prevent deletion." });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to delete company", details = ex.Message });
        }
    }
}