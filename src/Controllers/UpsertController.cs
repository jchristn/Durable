using Microsoft.AspNetCore.Mvc;
using Durable;
using Test.Shared;
using System.Diagnostics;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class UpsertController : ControllerBase
{
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;
    private readonly IRepository<Person> _personRepo;
    private readonly IRepository<Company> _companyRepo;

    public UpsertController(
        IRepository<Author> authorRepo,
        IRepository<Book> bookRepo,
        IRepository<Person> personRepo,
        IRepository<Company> companyRepo)
    {
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
        _personRepo = personRepo;
        _companyRepo = companyRepo;
    }

    public class UpsertResult
    {
        public bool IsNew { get; set; }
        public string Operation { get; set; }
        public object Entity { get; set; }
        public long ExecutionTimeMs { get; set; }
        public string Message { get; set; }
    }

    public class BulkUpsertResult
    {
        public int TotalRecords { get; set; }
        public int NewRecords { get; set; }
        public int UpdatedRecords { get; set; }
        public long ExecutionTimeMs { get; set; }
        public List<object> Entities { get; set; }
    }

    public class PerformanceComparison
    {
        public string Scenario { get; set; }
        public long UpsertTimeMs { get; set; }
        public long TraditionalTimeMs { get; set; }
        public double PerformanceGain { get; set; }
        public string Summary { get; set; }
    }

    [HttpPost("author")]
    public async Task<IActionResult> UpsertAuthor([FromBody] Author author)
    {
        if (author == null)
            return BadRequest("Author data is required");

        if (string.IsNullOrWhiteSpace(author.Name))
            return BadRequest("Author name is required");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Check if author exists (for demonstration purposes)
            var existingAuthor = author.Id > 0 
                ? await _authorRepo.ReadByIdAsync(author.Id) 
                : null;
            
            bool isNew = existingAuthor == null;

            // Validate company ID if provided
            if (author.CompanyId.HasValue)
            {
                var companyExists = await _companyRepo.ExistsAsync(c => c.Id == author.CompanyId.Value);
                if (!companyExists)
                    return BadRequest($"Company with ID {author.CompanyId.Value} does not exist");
            }

            // Perform upsert operation
            var upsertedAuthor = await _authorRepo.UpsertAsync(author);
            stopwatch.Stop();

            var result = new UpsertResult
            {
                IsNew = isNew,
                Operation = isNew ? "INSERT" : "UPDATE",
                Entity = upsertedAuthor,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = isNew 
                    ? $"Created new author with ID {upsertedAuthor.Id}"
                    : $"Updated existing author with ID {upsertedAuthor.Id}"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to upsert author", details = ex.Message });
        }
    }

    [HttpPost("authors/bulk")]
    public async Task<IActionResult> BulkUpsertAuthors([FromBody] List<Author> authors)
    {
        if (authors == null || !authors.Any())
            return BadRequest("Authors list is required and cannot be empty");

        if (authors.Count > 100)
            return BadRequest("Maximum 100 authors can be upserted in a single batch");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Validate all company IDs exist
            var companyIds = authors.Where(a => a.CompanyId.HasValue)
                .Select(a => a.CompanyId.Value)
                .Distinct();
            
            foreach (var companyId in companyIds)
            {
                var companyExists = await _companyRepo.ExistsAsync(c => c.Id == companyId);
                if (!companyExists)
                    return BadRequest($"Company with ID {companyId} does not exist");
            }

            // Track which records are new vs updates
            var existingIds = new HashSet<int>();
            foreach (var author in authors.Where(a => a.Id > 0))
            {
                if (await _authorRepo.ExistsByIdAsync(author.Id))
                    existingIds.Add(author.Id);
            }

            // Perform bulk upsert
            var upsertedAuthors = await _authorRepo.UpsertManyAsync(authors);
            stopwatch.Stop();

            var authorList = upsertedAuthors.ToList();
            var newCount = authorList.Count(a => !existingIds.Contains(a.Id));
            var updateCount = authorList.Count(a => existingIds.Contains(a.Id));

            var result = new BulkUpsertResult
            {
                TotalRecords = authorList.Count,
                NewRecords = newCount,
                UpdatedRecords = updateCount,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Entities = authorList.Cast<object>().ToList()
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to bulk upsert authors", details = ex.Message });
        }
    }

    [HttpPost("person")]
    public async Task<IActionResult> UpsertPerson([FromBody] Person person)
    {
        if (person == null)
            return BadRequest("Person data is required");

        if (string.IsNullOrWhiteSpace(person.FirstName) || string.IsNullOrWhiteSpace(person.LastName))
            return BadRequest("First name and last name are required");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // For Person, we might want to upsert based on email uniqueness
            Person existingPerson = null;
            if (!string.IsNullOrWhiteSpace(person.Email))
            {
                existingPerson = await _personRepo.ReadFirstOrDefaultAsync(p => p.Email == person.Email);
                if (existingPerson != null)
                {
                    // Preserve the existing ID for update
                    person.Id = existingPerson.Id;
                }
            }

            var upsertedPerson = await _personRepo.UpsertAsync(person);
            stopwatch.Stop();

            var result = new UpsertResult
            {
                IsNew = existingPerson == null,
                Operation = existingPerson == null ? "INSERT" : "UPDATE (matched by email)",
                Entity = upsertedPerson,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = existingPerson == null
                    ? $"Created new person with ID {upsertedPerson.Id}"
                    : $"Updated existing person with email {upsertedPerson.Email}"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to upsert person", details = ex.Message });
        }
    }

    [HttpPost("book")]
    public async Task<IActionResult> UpsertBook([FromBody] Book book)
    {
        if (book == null)
            return BadRequest("Book data is required");

        if (string.IsNullOrWhiteSpace(book.Title))
            return BadRequest("Book title is required");

        if (book.AuthorId <= 0)
            return BadRequest("Valid author ID is required");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Validate author exists
            var authorExists = await _authorRepo.ExistsAsync(a => a.Id == book.AuthorId);
            if (!authorExists)
                return BadRequest($"Author with ID {book.AuthorId} does not exist");

            // Validate publisher if provided
            if (book.PublisherId.HasValue)
            {
                var publisherExists = await _companyRepo.ExistsAsync(c => c.Id == book.PublisherId.Value);
                if (!publisherExists)
                    return BadRequest($"Publisher with ID {book.PublisherId.Value} does not exist");
            }

            // Check if updating existing book
            var existingBook = book.Id > 0 ? await _bookRepo.ReadByIdAsync(book.Id) : null;

            var upsertedBook = await _bookRepo.UpsertAsync(book);
            stopwatch.Stop();

            var result = new UpsertResult
            {
                IsNew = existingBook == null,
                Operation = existingBook == null ? "INSERT" : "UPDATE",
                Entity = upsertedBook,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = existingBook == null
                    ? $"Created new book '{upsertedBook.Title}' with ID {upsertedBook.Id}"
                    : $"Updated existing book '{upsertedBook.Title}' with ID {upsertedBook.Id}"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to upsert book", details = ex.Message });
        }
    }

    [HttpPost("people/bulk")]
    public async Task<IActionResult> BulkUpsertPeople([FromBody] List<Person> people)
    {
        if (people == null || !people.Any())
            return BadRequest("People list is required and cannot be empty");

        if (people.Count > 500)
            return BadRequest("Maximum 500 people can be upserted in a single batch");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // For people with emails, check which ones exist
            var emailToId = new Dictionary<string, int>();
            var peopleWithEmails = people.Where(p => !string.IsNullOrWhiteSpace(p.Email)).ToList();
            
            foreach (var person in peopleWithEmails)
            {
                var existing = await _personRepo.ReadFirstOrDefaultAsync(p => p.Email == person.Email);
                if (existing != null)
                {
                    emailToId[person.Email] = existing.Id;
                    person.Id = existing.Id; // Set ID for update
                }
            }

            // Perform bulk upsert
            var upsertedPeople = await _personRepo.UpsertManyAsync(people);
            stopwatch.Stop();

            var peopleList = upsertedPeople.ToList();
            var updateCount = emailToId.Count;
            var newCount = peopleList.Count - updateCount;

            var result = new BulkUpsertResult
            {
                TotalRecords = peopleList.Count,
                NewRecords = newCount,
                UpdatedRecords = updateCount,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Entities = peopleList.Cast<object>().ToList()
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to bulk upsert people", details = ex.Message });
        }
    }

    [HttpPost("company")]
    public async Task<IActionResult> UpsertCompany([FromBody] Company company)
    {
        if (company == null)
            return BadRequest("Company data is required");

        if (string.IsNullOrWhiteSpace(company.Name))
            return BadRequest("Company name is required");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Check if company with same name exists (business rule: company names are unique)
            var existingCompany = await _companyRepo.ReadFirstOrDefaultAsync(c => c.Name == company.Name);
            if (existingCompany != null)
            {
                // Update the existing company
                company.Id = existingCompany.Id;
            }

            var upsertedCompany = await _companyRepo.UpsertAsync(company);
            stopwatch.Stop();

            var result = new UpsertResult
            {
                IsNew = existingCompany == null,
                Operation = existingCompany == null ? "INSERT" : "UPDATE (matched by name)",
                Entity = upsertedCompany,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = existingCompany == null
                    ? $"Created new company '{upsertedCompany.Name}' with ID {upsertedCompany.Id}"
                    : $"Updated existing company '{upsertedCompany.Name}' with ID {upsertedCompany.Id}"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to upsert company", details = ex.Message });
        }
    }

    [HttpGet("performance-comparison")]
    public async Task<IActionResult> CompareUpsertPerformance([FromQuery] int recordCount = 50)
    {
        if (recordCount < 10 || recordCount > 200)
            return BadRequest("Record count must be between 10 and 200");

        try
        {
            var comparisons = new List<PerformanceComparison>();

            // Test 1: Single record update scenario
            var testAuthor = new Author { Id = 1, Name = "Test Author Updated", CompanyId = 1 };
            
            // Traditional approach: Check existence then update
            var traditionalStopwatch = Stopwatch.StartNew();
            var exists = await _authorRepo.ExistsByIdAsync(testAuthor.Id);
            if (exists)
                await _authorRepo.UpdateAsync(testAuthor);
            else
                await _authorRepo.CreateAsync(testAuthor);
            traditionalStopwatch.Stop();

            // Upsert approach
            var upsertStopwatch = Stopwatch.StartNew();
            await _authorRepo.UpsertAsync(testAuthor);
            upsertStopwatch.Stop();

            comparisons.Add(new PerformanceComparison
            {
                Scenario = "Single Record Update",
                TraditionalTimeMs = traditionalStopwatch.ElapsedMilliseconds,
                UpsertTimeMs = upsertStopwatch.ElapsedMilliseconds,
                PerformanceGain = traditionalStopwatch.ElapsedMilliseconds > 0 
                    ? Math.Round(((double)traditionalStopwatch.ElapsedMilliseconds - upsertStopwatch.ElapsedMilliseconds) / traditionalStopwatch.ElapsedMilliseconds * 100, 2)
                    : 0,
                Summary = "Upsert eliminates the need for existence check"
            });

            // Test 2: Bulk mixed insert/update scenario
            var testPeople = new List<Person>();
            for (int i = 1; i <= recordCount; i++)
            {
                testPeople.Add(new Person
                {
                    Id = i <= recordCount / 2 ? 0 : i, // Half new, half existing
                    FirstName = $"PerfTest{i}",
                    LastName = $"User{i}",
                    Age = 30,
                    Email = $"perftest{i}@test.com",
                    Department = "TestDept",
                    Salary = 50000
                });
            }

            // Traditional approach: Separate creates and updates
            traditionalStopwatch = Stopwatch.StartNew();
            var toCreate = new List<Person>();
            var toUpdate = new List<Person>();
            
            foreach (var person in testPeople)
            {
                if (person.Id == 0)
                    toCreate.Add(person);
                else if (await _personRepo.ExistsByIdAsync(person.Id))
                    toUpdate.Add(person);
                else
                    toCreate.Add(person);
            }
            
            if (toCreate.Any())
                await _personRepo.CreateManyAsync(toCreate);
            foreach (var person in toUpdate)
                await _personRepo.UpdateAsync(person);
            traditionalStopwatch.Stop();

            // Upsert approach
            upsertStopwatch = Stopwatch.StartNew();
            await _personRepo.UpsertManyAsync(testPeople);
            upsertStopwatch.Stop();

            comparisons.Add(new PerformanceComparison
            {
                Scenario = $"Bulk Mixed Operations ({recordCount} records)",
                TraditionalTimeMs = traditionalStopwatch.ElapsedMilliseconds,
                UpsertTimeMs = upsertStopwatch.ElapsedMilliseconds,
                PerformanceGain = traditionalStopwatch.ElapsedMilliseconds > 0
                    ? Math.Round(((double)traditionalStopwatch.ElapsedMilliseconds - upsertStopwatch.ElapsedMilliseconds) / traditionalStopwatch.ElapsedMilliseconds * 100, 2)
                    : 0,
                Summary = "Upsert handles mixed insert/update in single operation"
            });

            // Clean up test data
            await _personRepo.BatchDeleteAsync(p => p.FirstName.StartsWith("PerfTest"));

            return Ok(new
            {
                comparisons,
                summary = new
                {
                    message = "Upsert operations typically provide better performance by eliminating existence checks and reducing round trips",
                    benefits = new[]
                    {
                        "Single database operation instead of check + insert/update",
                        "Atomic operation - no race conditions",
                        "Simplified code - no conditional logic needed",
                        "Better performance for bulk operations"
                    }
                }
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to run performance comparison", details = ex.Message });
        }
    }

    [HttpPost("sync-data")]
    public async Task<IActionResult> SyncData([FromBody] SyncRequest request)
    {
        if (request == null || request.Authors == null)
            return BadRequest("Sync request with authors is required");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate a data synchronization scenario where we receive data from external source
            // and need to insert new records or update existing ones based on some business key
            
            var syncResults = new List<object>();
            
            foreach (var author in request.Authors)
            {
                // Use name as business key for matching
                var existing = await _authorRepo.ReadFirstOrDefaultAsync(a => a.Name == author.Name);
                if (existing != null)
                {
                    author.Id = existing.Id; // Preserve ID for update
                }
                
                var upserted = await _authorRepo.UpsertAsync(author);
                syncResults.Add(new
                {
                    Name = upserted.Name,
                    Id = upserted.Id,
                    Operation = existing == null ? "Created" : "Updated"
                });
            }

            stopwatch.Stop();

            return Ok(new
            {
                message = "Data synchronization completed",
                recordsProcessed = request.Authors.Count,
                executionTimeMs = stopwatch.ElapsedMilliseconds,
                results = syncResults
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to sync data", details = ex.Message });
        }
    }

    public class SyncRequest
    {
        public List<Author> Authors { get; set; }
    }
}