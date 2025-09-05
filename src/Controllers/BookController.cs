using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class BookController : ControllerBase
{
    private readonly IRepository<Book> _bookRepo;
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Company> _companyRepo;

    public BookController(IRepository<Book> bookRepo, IRepository<Author> authorRepo, IRepository<Company> companyRepo)
    {
        _bookRepo = bookRepo;
        _authorRepo = authorRepo;
        _companyRepo = companyRepo;
    }

    private async Task<(bool IsValid, string ErrorMessage)> ValidateForeignKeysAsync(Book book)
    {
        // Validate required AuthorId
        var authorExists = await _authorRepo.ExistsAsync(a => a.Id == book.AuthorId);
        if (!authorExists)
            return (false, $"Author with ID {book.AuthorId} does not exist");

        // Validate optional PublisherId
        if (book.PublisherId.HasValue)
        {
            var publisherExists = await _companyRepo.ExistsAsync(c => c.Id == book.PublisherId.Value);
            if (!publisherExists)
                return (false, $"Publisher with ID {book.PublisherId.Value} does not exist");
        }

        return (true, string.Empty);
    }

    [HttpGet]
    public async Task<IActionResult> GetBooks([FromQuery] bool includeAuthor = false, [FromQuery] bool includePublisher = false)
    {
        try
        {
            var query = _bookRepo.Query();
            
            if (includeAuthor)
                query = query.Include(b => b.Author);
            
            if (includePublisher)
                query = query.Include(b => b.Publisher);

            var books = query.Execute().ToList();
            return Ok(books);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve books", details = ex.Message });
        }
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetBook(int id, [FromQuery] bool includeAuthor = false, [FromQuery] bool includePublisher = false)
    {
        if (id <= 0)
            return BadRequest("Invalid book ID");

        try
        {
            var query = _bookRepo.Query().Where(b => b.Id == id);
            
            if (includeAuthor)
                query = query.Include(b => b.Author);
            
            if (includePublisher)
                query = query.Include(b => b.Publisher);

            var book = query.Execute().FirstOrDefault();
            if (book == null)
                return NotFound($"Book with ID {id} not found");
            return Ok(book);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve book", details = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> CreateBook([FromBody] Book book)
    {
        if (book == null)
            return BadRequest("Book data is required");

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
        var (isValid, errorMessage) = await ValidateForeignKeysAsync(book);
        if (!isValid)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = errorMessage });
        }

        try
        {
            var created = await _bookRepo.CreateAsync(book);
            return Ok(created);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = "Referenced author or publisher does not exist" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to create book", details = ex.Message });
        }
    }

    [HttpPut("{id}")]
    public async Task<IActionResult> UpdateBook(int id, [FromBody] Book book)
    {
        if (id <= 0)
            return BadRequest("Invalid book ID");
        if (book == null)
            return BadRequest("Book data is required");

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
        var (isValid, errorMessage) = await ValidateForeignKeysAsync(book);
        if (!isValid)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = errorMessage });
        }

        try
        {
            // Check if book exists
            var existing = await _bookRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Book with ID {id} not found");

            book.Id = id;
            var updated = await _bookRepo.UpdateAsync(book);
            return Ok(updated);
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            return BadRequest(new { error = "Invalid foreign key reference", details = "Referenced author or publisher does not exist" });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to update book", details = ex.Message });
        }
    }

    [HttpDelete("{id}")]
    public async Task<IActionResult> DeleteBook(int id)
    {
        if (id <= 0)
            return BadRequest("Invalid book ID");

        try
        {
            // Check if book exists
            var existing = await _bookRepo.ReadByIdAsync(id);
            if (existing == null)
                return NotFound($"Book with ID {id} not found");

            await _bookRepo.DeleteByIdAsync(id);
            return NoContent();
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to delete book", details = ex.Message });
        }
    }
}