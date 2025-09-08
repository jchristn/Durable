using Microsoft.AspNetCore.Mvc;
using Durable;
using Test.Shared;
using System.Diagnostics;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class BatchOperationsController : ControllerBase
{
    private readonly IRepository<Person> _personRepo;
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;
    private readonly IRepository<Company> _companyRepo;

    public BatchOperationsController(
        IRepository<Person> personRepo, 
        IRepository<Author> authorRepo,
        IRepository<Book> bookRepo,
        IRepository<Company> companyRepo)
    {
        _personRepo = personRepo;
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
        _companyRepo = companyRepo;
    }

    public class BatchOperationResult
    {
        public int RecordsAffected { get; set; }
        public long ExecutionTimeMs { get; set; }
        public string Operation { get; set; }
        public string Message { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }

    public class SalaryUpdateRequest
    {
        public string Department { get; set; }
        public decimal PercentageIncrease { get; set; }
        public decimal? FlatIncrease { get; set; }
        public decimal? MinimumSalary { get; set; }
        public decimal? MaximumSalary { get; set; }
    }

    public class BulkDeleteRequest
    {
        public string Department { get; set; }
        public decimal? MaxSalary { get; set; }
        public int? MaxAge { get; set; }
        public bool DryRun { get; set; } = true;
    }

    [HttpPut("update-salaries")]
    public async Task<IActionResult> BulkUpdateSalaries([FromBody] SalaryUpdateRequest request)
    {
        if (request == null)
            return BadRequest("Salary update request is required");

        if (string.IsNullOrWhiteSpace(request.Department))
            return BadRequest("Department is required");

        if (request.PercentageIncrease <= 0 && (!request.FlatIncrease.HasValue || request.FlatIncrease <= 0))
            return BadRequest("Either percentage increase or flat increase must be positive");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Using BatchUpdate for high-performance bulk operations
            int recordsUpdated;
            
            if (request.PercentageIncrease > 0)
            {
                // Percentage-based salary increase
                recordsUpdated = await _personRepo.BatchUpdateAsync(
                    p => p.Department == request.Department &&
                         (request.MinimumSalary == null || p.Salary >= request.MinimumSalary) &&
                         (request.MaximumSalary == null || p.Salary <= request.MaximumSalary),
                    p => new Person 
                    { 
                        Id = p.Id,
                        FirstName = p.FirstName,
                        LastName = p.LastName,
                        Age = p.Age,
                        Email = p.Email,
                        Department = p.Department,
                        Salary = p.Salary * (1 + (request.PercentageIncrease / 100))
                    });
            }
            else
            {
                // Flat salary increase
                recordsUpdated = await _personRepo.BatchUpdateAsync(
                    p => p.Department == request.Department &&
                         (request.MinimumSalary == null || p.Salary >= request.MinimumSalary) &&
                         (request.MaximumSalary == null || p.Salary <= request.MaximumSalary),
                    p => new Person 
                    { 
                        Id = p.Id,
                        FirstName = p.FirstName,
                        LastName = p.LastName,
                        Age = p.Age,
                        Email = p.Email,
                        Department = p.Department,
                        Salary = p.Salary + request.FlatIncrease.Value
                    });
            }

            stopwatch.Stop();

            var result = new BatchOperationResult
            {
                RecordsAffected = recordsUpdated,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Operation = "Bulk Salary Update",
                Message = $"Updated salaries for {recordsUpdated} employees in {request.Department} department"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to perform bulk salary update", details = ex.Message });
        }
    }

    [HttpPost("update-salaries-by-field")]
    public async Task<IActionResult> BulkUpdateSalariesByField(
        [FromQuery] string department,
        [FromQuery] decimal newSalary)
    {
        if (string.IsNullOrWhiteSpace(department))
            return BadRequest("Department parameter is required");

        if (newSalary <= 0)
            return BadRequest("New salary must be positive");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Using UpdateFieldAsync for simple field updates
            var recordsUpdated = await _personRepo.UpdateFieldAsync(
                p => p.Department == department,
                p => p.Salary,
                newSalary);

            stopwatch.Stop();

            var result = new BatchOperationResult
            {
                RecordsAffected = recordsUpdated,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Operation = "Bulk Field Update",
                Message = $"Set salary to ${newSalary:N2} for {recordsUpdated} employees in {department} department"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to perform bulk field update", details = ex.Message });
        }
    }

    [HttpDelete("bulk-delete")]
    public async Task<IActionResult> BulkDelete([FromBody] BulkDeleteRequest request)
    {
        if (request == null)
            return BadRequest("Bulk delete request is required");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Build predicate based on criteria
            System.Linq.Expressions.Expression<Func<Person, bool>> predicate = p => true;

            if (!string.IsNullOrWhiteSpace(request.Department))
            {
                predicate = p => p.Department == request.Department;
            }

            if (request.MaxSalary.HasValue)
            {
                var currentPredicate = predicate;
                predicate = p => currentPredicate.Compile()(p) && p.Salary <= request.MaxSalary.Value;
            }

            if (request.MaxAge.HasValue)
            {
                var currentPredicate = predicate;
                predicate = p => currentPredicate.Compile()(p) && p.Age <= request.MaxAge.Value;
            }

            int recordsAffected;
            
            if (request.DryRun)
            {
                // Dry run - just count how many would be deleted
                recordsAffected = await _personRepo.CountAsync(predicate);
                stopwatch.Stop();

                var dryRunResult = new BatchOperationResult
                {
                    RecordsAffected = recordsAffected,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    Operation = "Bulk Delete (Dry Run)",
                    Message = $"Would delete {recordsAffected} records matching the criteria"
                };

                return Ok(dryRunResult);
            }
            else
            {
                // Actually perform the deletion using BatchDelete
                recordsAffected = await _personRepo.BatchDeleteAsync(predicate);
                stopwatch.Stop();

                var result = new BatchOperationResult
                {
                    RecordsAffected = recordsAffected,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    Operation = "Bulk Delete",
                    Message = $"Deleted {recordsAffected} records matching the criteria"
                };

                return Ok(result);
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to perform bulk delete", details = ex.Message });
        }
    }

    [HttpPost("bulk-create-people")]
    public async Task<IActionResult> BulkCreatePeople([FromBody] List<Person> people)
    {
        if (people == null || !people.Any())
            return BadRequest("People list is required and cannot be empty");

        if (people.Count > 1000)
            return BadRequest("Maximum 1000 people can be created in a single batch");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Use CreateManyAsync for bulk insertion
            var createdPeople = await _personRepo.CreateManyAsync(people);
            stopwatch.Stop();

            var result = new BatchOperationResult
            {
                RecordsAffected = createdPeople.Count(),
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Operation = "Bulk Create",
                Message = $"Created {createdPeople.Count()} people records"
            };

            return Ok(new { result, createdPeople });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to perform bulk create", details = ex.Message });
        }
    }

    [HttpPost("bulk-create-authors")]
    public async Task<IActionResult> BulkCreateAuthors([FromBody] List<Author> authors)
    {
        if (authors == null || !authors.Any())
            return BadRequest("Authors list is required and cannot be empty");

        if (authors.Count > 100)
            return BadRequest("Maximum 100 authors can be created in a single batch");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Validate that all company IDs exist if specified
            var companyIds = authors.Where(a => a.CompanyId.HasValue).Select(a => a.CompanyId.Value).Distinct();
            foreach (var companyId in companyIds)
            {
                var companyExists = await _companyRepo.ExistsAsync(c => c.Id == companyId);
                if (!companyExists)
                    return BadRequest($"Company with ID {companyId} does not exist");
            }

            var createdAuthors = await _authorRepo.CreateManyAsync(authors);
            stopwatch.Stop();

            var result = new BatchOperationResult
            {
                RecordsAffected = createdAuthors.Count(),
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Operation = "Bulk Create Authors",
                Message = $"Created {createdAuthors.Count()} author records"
            };

            return Ok(new { result, createdAuthors });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to perform bulk create authors", details = ex.Message });
        }
    }

    [HttpDelete("cleanup-test-data")]
    public async Task<IActionResult> CleanupTestData([FromQuery] bool confirm = false)
    {
        if (!confirm)
            return BadRequest("Add ?confirm=true to actually perform the cleanup");

        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Delete test people (those with "Test" in their name)
            var deletedPeople = await _personRepo.BatchDeleteAsync(p => 
                p.FirstName.Contains("Test") || 
                p.LastName.Contains("Test") ||
                p.Email.Contains("test"));

            // Delete test authors
            var deletedAuthors = await _authorRepo.BatchDeleteAsync(a => 
                a.Name.Contains("Test"));

            stopwatch.Stop();

            var result = new BatchOperationResult
            {
                RecordsAffected = deletedPeople + deletedAuthors,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Operation = "Cleanup Test Data",
                Message = $"Deleted {deletedPeople} test people and {deletedAuthors} test authors"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return StatusCode(500, new { error = "Failed to cleanup test data", details = ex.Message });
        }
    }

    [HttpGet("performance-comparison")]
    public async Task<IActionResult> PerformanceComparison([FromQuery] int recordCount = 100)
    {
        if (recordCount < 10 || recordCount > 1000)
            return BadRequest("Record count must be between 10 and 1000");

        try
        {
            var results = new
            {
                RecordCount = recordCount,
                SingleOperations = new { },
                BatchOperations = new { }
            };

            // Generate test data
            var testPeople = Enumerable.Range(1, recordCount)
                .Select(i => new Person
                {
                    FirstName = $"TestUser{i}",
                    LastName = $"Batch{i}",
                    Age = 25 + (i % 40),
                    Email = $"testuser{i}@batch.com",
                    Department = i % 2 == 0 ? "TestDept1" : "TestDept2",
                    Salary = 50000 + (i * 100)
                })
                .ToList();

            // Test batch create
            var batchCreateStopwatch = Stopwatch.StartNew();
            var createdPeople = await _personRepo.CreateManyAsync(testPeople);
            batchCreateStopwatch.Stop();

            // Test batch update
            var batchUpdateStopwatch = Stopwatch.StartNew();
            var updatedCount = await _personRepo.BatchUpdateAsync(
                p => p.Department == "TestDept1",
                p => new Person 
                { 
                    Id = p.Id,
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Age = p.Age,
                    Email = p.Email,
                    Department = p.Department,
                    Salary = p.Salary * 1.1m
                });
            batchUpdateStopwatch.Stop();

            // Test batch delete
            var batchDeleteStopwatch = Stopwatch.StartNew();
            var deletedCount = await _personRepo.BatchDeleteAsync(p => p.FirstName.Contains("TestUser"));
            batchDeleteStopwatch.Stop();

            var performanceResults = new
            {
                RecordCount = recordCount,
                BatchOperations = new
                {
                    CreateTimeMs = batchCreateStopwatch.ElapsedMilliseconds,
                    UpdateTimeMs = batchUpdateStopwatch.ElapsedMilliseconds,
                    DeleteTimeMs = batchDeleteStopwatch.ElapsedMilliseconds,
                    TotalTimeMs = batchCreateStopwatch.ElapsedMilliseconds + batchUpdateStopwatch.ElapsedMilliseconds + batchDeleteStopwatch.ElapsedMilliseconds,
                    RecordsCreated = createdPeople.Count(),
                    RecordsUpdated = updatedCount,
                    RecordsDeleted = deletedCount
                },
                Summary = new
                {
                    AverageCreateTimePerRecord = (double)batchCreateStopwatch.ElapsedMilliseconds / recordCount,
                    AverageUpdateTimePerRecord = updatedCount > 0 ? (double)batchUpdateStopwatch.ElapsedMilliseconds / updatedCount : 0,
                    AverageDeleteTimePerRecord = deletedCount > 0 ? (double)batchDeleteStopwatch.ElapsedMilliseconds / deletedCount : 0
                }
            };

            return Ok(performanceResults);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to run performance comparison", details = ex.Message });
        }
    }
}