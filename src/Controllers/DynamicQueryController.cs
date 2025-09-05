using Microsoft.AspNetCore.Mvc;
using System.Linq.Dynamic.Core;
using System.Linq.Dynamic.Core.Exceptions;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DynamicQueryController : ControllerBase
{
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;

    public DynamicQueryController(IRepository<Author> authorRepo, IRepository<Book> bookRepo)
    {
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
    }

    public class QueryRequest
    {
        public string? Where { get; set; }
        public string? OrderBy { get; set; }
        public string? Select { get; set; }
        public int? Skip { get; set; }
        public int? Take { get; set; }
        public string[]? Include { get; set; }
    }

    [HttpPost("authors/query")]
    public async Task<IActionResult> QueryAuthors([FromBody] QueryRequest request)
    {
        if (request == null)
            return BadRequest("Query request is required");

        // Validate pagination parameters
        if (request.Skip.HasValue && request.Skip.Value < 0)
            return BadRequest("Skip parameter must be non-negative");
        if (request.Take.HasValue && (request.Take.Value < 1 || request.Take.Value > 1000))
            return BadRequest("Take parameter must be between 1 and 1000");

        // Validate include parameters
        if (request.Include != null && request.Include.Length > 5)
            return BadRequest("Maximum 5 include paths allowed");

        try
        {
            // Start with query builder instead of ReadAllAsync to support includes
            var authorQuery = _authorRepo.Query();
            
            // Apply includes first
            if (request.Include != null && request.Include.Length > 0)
            {
                foreach (var includePath in request.Include)
                {
                    if (string.IsNullOrWhiteSpace(includePath))
                        continue;
                        
                    // Support common navigation properties
                    switch (includePath.ToLowerInvariant())
                    {
                        case "company":
                            authorQuery = authorQuery.Include(a => a.Company);
                            break;
                        case "books":
                            authorQuery = authorQuery.Include(a => a.Books);
                            break;
                        case "categories":
                            authorQuery = authorQuery.Include(a => a.Categories);
                            break;
                        case "books.publisher":
                            authorQuery = authorQuery.Include(a => a.Books).ThenInclude<Book, Company>(b => b.Publisher);
                            break;
                        case "company.publishedbooks":
                            authorQuery = authorQuery.Include(a => a.Company).ThenInclude<Company, List<Book>>(c => c.PublishedBooks);
                            break;
                        default:
                            return BadRequest($"Unsupported include path: {includePath}. Supported paths: company, books, categories, books.publisher, company.publishedbooks");
                    }
                }
            }
            
            // Execute the query to get the base results
            var baseResults = authorQuery.Execute().AsQueryable();
            
            // Apply dynamic where clause if provided
            if (!string.IsNullOrEmpty(request.Where))
                baseResults = baseResults.Where(request.Where);

            // Apply dynamic ordering
            if (!string.IsNullOrEmpty(request.OrderBy))
                baseResults = baseResults.OrderBy(request.OrderBy);
            else
                baseResults = baseResults.OrderBy(a => a.Name); // Default ordering

            // Apply pagination
            if (request.Skip.HasValue)
                baseResults = baseResults.Skip(request.Skip.Value);

            if (request.Take.HasValue)
                baseResults = baseResults.Take(request.Take.Value);
            else
                baseResults = baseResults.Take(100); // Default limit

            // Get the final results
            var results = baseResults.ToList();
            
            // Apply dynamic select if provided
            if (!string.IsNullOrEmpty(request.Select))
            {
                var selectQuery = results.AsQueryable();
                return Ok(selectQuery.Select(request.Select).ToDynamicList());
            }

            return Ok(results);
        }
        catch (ParseException ex)
        {
            return BadRequest(new { error = "Invalid dynamic query syntax", details = ex.Message });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute dynamic query", details = ex.Message });
        }
    }

    [HttpPost("books/query")]
    public async Task<IActionResult> QueryBooks([FromBody] QueryRequest request)
    {
        if (request == null)
            return BadRequest("Query request is required");

        // Validate pagination parameters
        if (request.Skip.HasValue && request.Skip.Value < 0)
            return BadRequest("Skip parameter must be non-negative");
        if (request.Take.HasValue && (request.Take.Value < 1 || request.Take.Value > 1000))
            return BadRequest("Take parameter must be between 1 and 1000");

        // Validate include parameters
        if (request.Include != null && request.Include.Length > 5)
            return BadRequest("Maximum 5 include paths allowed");

        try
        {
            // Start with query builder for books
            var bookQuery = _bookRepo.Query();
            
            // Apply includes first
            if (request.Include != null && request.Include.Length > 0)
            {
                foreach (var includePath in request.Include)
                {
                    if (string.IsNullOrWhiteSpace(includePath))
                        continue;
                        
                    // Support common navigation properties for books
                    switch (includePath.ToLowerInvariant())
                    {
                        case "author":
                            bookQuery = bookQuery.Include(b => b.Author);
                            break;
                        case "publisher":
                            bookQuery = bookQuery.Include(b => b.Publisher);
                            break;
                        case "author.company":
                            bookQuery = bookQuery.Include(b => b.Author).ThenInclude<Author, Company>(a => a.Company);
                            break;
                        case "author.categories":
                            bookQuery = bookQuery.Include(b => b.Author).ThenInclude<Author, List<Category>>(a => a.Categories);
                            break;
                        default:
                            return BadRequest($"Unsupported include path: {includePath}. Supported paths: author, publisher, author.company, author.categories");
                    }
                }
            }
            
            // Execute the query to get the base results
            var baseResults = bookQuery.Execute().AsQueryable();
            
            // Apply dynamic where clause if provided
            if (!string.IsNullOrEmpty(request.Where))
                baseResults = baseResults.Where(request.Where);

            // Apply dynamic ordering
            if (!string.IsNullOrEmpty(request.OrderBy))
                baseResults = baseResults.OrderBy(request.OrderBy);
            else
                baseResults = baseResults.OrderBy(b => b.Title); // Default ordering

            // Apply pagination
            if (request.Skip.HasValue)
                baseResults = baseResults.Skip(request.Skip.Value);

            if (request.Take.HasValue)
                baseResults = baseResults.Take(request.Take.Value);
            else
                baseResults = baseResults.Take(100); // Default limit

            // Get the final results
            var results = baseResults.ToList();
            
            // Apply dynamic select if provided
            if (!string.IsNullOrEmpty(request.Select))
            {
                var selectQuery = results.AsQueryable();
                return Ok(selectQuery.Select(request.Select).ToDynamicList());
            }

            return Ok(results);
        }
        catch (ParseException ex)
        {
            return BadRequest(new { error = "Invalid dynamic query syntax", details = ex.Message });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute dynamic query", details = ex.Message });
        }
    }

    [HttpPost("raw-sql")]
    public async Task<IActionResult> ExecuteRawQuery([FromBody] string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return BadRequest("SQL query is required");

        // Basic SQL injection protection - only allow SELECT statements
        var trimmedSql = sql.Trim().ToUpperInvariant();
        if (!trimmedSql.StartsWith("SELECT"))
            return BadRequest("Only SELECT statements are allowed");

        // Prevent dangerous SQL operations
        var dangerousKeywords = new[] { "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "EXEC", "EXECUTE" };
        if (dangerousKeywords.Any(keyword => trimmedSql.Contains(keyword)))
            return BadRequest("SQL query contains prohibited operations");

        try
        {
            // Use the FromSqlAsync method for raw SQL queries
            var results = await _authorRepo.FromSqlAsync(sql).ToListAsync();
            return Ok(results);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute raw SQL query", details = ex.Message });
        }
    }
}

// Example Postman requests:
/*
POST /api/dynamicquery/authors/query
{
  "where": "Name.Contains(\"John\") && Id > 5",
  "orderBy": "Name desc, Id",
  "select": "new { Id, Name, BookCount = Books.Count() }",
  "skip": 10,
  "take": 20
}
*/