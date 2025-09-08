using Microsoft.AspNetCore.Mvc;
using Durable;
using Test.Shared;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class PerformanceController : ControllerBase
{
    private readonly IRepository<Person> _personRepo;
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;
    private readonly IRepository<Company> _companyRepo;
    private readonly IRepository<Category> _categoryRepo;
    private readonly IRepository<AuthorCategory> _authorCategoryRepo;

    public PerformanceController(
        IRepository<Person> personRepo,
        IRepository<Author> authorRepo,
        IRepository<Book> bookRepo,
        IRepository<Company> companyRepo,
        IRepository<Category> categoryRepo,
        IRepository<AuthorCategory> authorCategoryRepo)
    {
        _personRepo = personRepo;
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
        _companyRepo = companyRepo;
        _categoryRepo = categoryRepo;
        _authorCategoryRepo = authorCategoryRepo;
    }

    public class PerformanceResult
    {
        public string TestName { get; set; }
        public string Description { get; set; }
        public Dictionary<string, MethodPerformance> Methods { get; set; } = new Dictionary<string, MethodPerformance>();
        public string Winner { get; set; }
        public double PerformanceGainPercent { get; set; }
        public List<string> Recommendations { get; set; } = new List<string>();
    }

    public class MethodPerformance
    {
        public long ExecutionTimeMs { get; set; }
        public int RecordsProcessed { get; set; }
        public double RecordsPerSecond { get; set; }
        public long MemoryUsedBytes { get; set; }
        public string Notes { get; set; }
    }

    public class BulkInsertRequest
    {
        public int RecordCount { get; set; } = 1000;
        public bool UseTransaction { get; set; } = true;
        public bool CleanupAfter { get; set; } = true;
    }

    [HttpPost("bulk-insert")]
    public async Task<IActionResult> BulkInsertPerformance([FromBody] BulkInsertRequest request)
    {
        if (request.RecordCount < 100 || request.RecordCount > 10000)
            return BadRequest("Record count must be between 100 and 10000");

        var result = new PerformanceResult
        {
            TestName = "Bulk Insert Performance",
            Description = $"Comparing different methods of inserting {request.RecordCount} records"
        };

        // Generate test data
        var testPeople = GenerateTestPeople(request.RecordCount);

        try
        {
            // Method 1: Individual inserts (worst case)
            var individualResult = await TestIndividualInserts(testPeople.Take(Math.Min(100, request.RecordCount)).ToList());
            result.Methods["Individual Inserts"] = individualResult;

            // Method 2: Individual inserts with transaction
            var transactionResult = await TestIndividualInsertsWithTransaction(testPeople.Take(Math.Min(500, request.RecordCount)).ToList());
            result.Methods["Individual with Transaction"] = transactionResult;

            // Method 3: Bulk insert with CreateManyAsync
            var bulkResult = await TestBulkInsert(testPeople);
            result.Methods["CreateManyAsync"] = bulkResult;

            // Method 4: Bulk insert with transaction
            var bulkTransactionResult = await TestBulkInsertWithTransaction(testPeople);
            result.Methods["CreateManyAsync with Transaction"] = bulkTransactionResult;

            // Determine winner
            var fastest = result.Methods.OrderBy(m => m.Value.ExecutionTimeMs).First();
            var slowest = result.Methods.OrderByDescending(m => m.Value.ExecutionTimeMs).First();
            
            result.Winner = fastest.Key;
            result.PerformanceGainPercent = Math.Round(
                ((double)slowest.Value.ExecutionTimeMs - fastest.Value.ExecutionTimeMs) / slowest.Value.ExecutionTimeMs * 100, 2);

            result.Recommendations.Add($"Use {result.Winner} for best performance");
            result.Recommendations.Add($"Performance gain of {result.PerformanceGainPercent}% over {slowest.Key}");
            result.Recommendations.Add("Always use CreateManyAsync for bulk operations");
            result.Recommendations.Add("Transactions improve consistency without significant performance cost");

            // Cleanup if requested
            if (request.CleanupAfter)
            {
                await _personRepo.BatchDeleteAsync(p => p.FirstName.StartsWith("PerfTest"));
            }

            return Ok(result);
        }
        catch (Exception ex)
        {
            // Cleanup on error
            await _personRepo.BatchDeleteAsync(p => p.FirstName.StartsWith("PerfTest"));
            return StatusCode(500, new { error = "Performance test failed", details = ex.Message });
        }
    }

    [HttpGet("query-optimization")]
    public async Task<IActionResult> QueryOptimizationExamples()
    {
        var results = new List<object>();

        // Example 1: Inefficient vs Efficient filtering
        var inefficientStopwatch = Stopwatch.StartNew();
        var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
        var filteredInMemory = allPeople.Where(p => p.Salary > 70000 && p.Department == "Engineering").ToList();
        inefficientStopwatch.Stop();

        var efficientStopwatch = Stopwatch.StartNew();
        var filteredInDb = await _personRepo.ReadManyAsync(p => p.Salary > 70000 && p.Department == "Engineering").ToListAsync();
        efficientStopwatch.Stop();

        results.Add(new
        {
            Test = "Filtering: In-Memory vs Database",
            Inefficient = new
            {
                Method = "ReadAllAsync() then filter in memory",
                TimeMs = inefficientStopwatch.ElapsedMilliseconds,
                RecordsRetrieved = allPeople.Count,
                RecordsAfterFilter = filteredInMemory.Count
            },
            Efficient = new
            {
                Method = "ReadManyAsync() with predicate",
                TimeMs = efficientStopwatch.ElapsedMilliseconds,
                RecordsRetrieved = filteredInDb.Count,
                Note = "Filter applied at database level"
            },
            Recommendation = "Always filter at the database level when possible"
        });

        // Example 2: Projection optimization
        var fullObjectStopwatch = Stopwatch.StartNew();
        var fullObjects = await _authorRepo.ReadAllAsync().ToListAsync();
        var namesFromFull = fullObjects.Select(a => new { a.Id, a.Name }).ToList();
        fullObjectStopwatch.Stop();

        var projectionStopwatch = Stopwatch.StartNew();
        var projectedQuery = _authorRepo.Query()
            .Select<Author>(a => new Author { Id = a.Id, Name = a.Name });
        var projectedResults = projectedQuery.Execute().ToList();
        projectionStopwatch.Stop();

        results.Add(new
        {
            Test = "Projection: Full Object vs Selected Fields",
            FullObject = new
            {
                Method = "Retrieve full objects then project",
                TimeMs = fullObjectStopwatch.ElapsedMilliseconds,
                DataTransferred = "All columns"
            },
            Optimized = new
            {
                Method = "Project at query level",
                TimeMs = projectionStopwatch.ElapsedMilliseconds,
                DataTransferred = "Only Id and Name columns"
            },
            Recommendation = "Use projection to reduce data transfer"
        });

        // Example 3: Pagination best practices
        var noPagingStopwatch = Stopwatch.StartNew();
        var allRecords = await _personRepo.ReadAllAsync().ToListAsync();
        var firstPage = allRecords.Take(10).ToList();
        noPagingStopwatch.Stop();

        var pagingStopwatch = Stopwatch.StartNew();
        var pagedRecords = _personRepo.Query()
            .OrderBy(p => p.Id)
            .Skip(0)
            .Take(10)
            .Execute()
            .ToList();
        pagingStopwatch.Stop();

        results.Add(new
        {
            Test = "Pagination: Client-side vs Server-side",
            ClientSide = new
            {
                Method = "Fetch all then take first 10",
                TimeMs = noPagingStopwatch.ElapsedMilliseconds,
                RecordsTransferred = allRecords.Count
            },
            ServerSide = new
            {
                Method = "Skip/Take at database level",
                TimeMs = pagingStopwatch.ElapsedMilliseconds,
                RecordsTransferred = 10
            },
            Recommendation = "Always use Skip/Take for pagination at database level"
        });

        return Ok(new
        {
            Title = "Query Optimization Examples",
            Results = results,
            GeneralRecommendations = new[]
            {
                "Filter early at the database level",
                "Use projections to reduce data transfer",
                "Implement server-side pagination",
                "Use appropriate indexes",
                "Batch operations when possible",
                "Use includes to avoid N+1 queries"
            }
        });
    }

    [HttpGet("n-plus-one")]
    public async Task<IActionResult> NPlusOneProblem([FromQuery] bool showProblem = true)
    {
        var stopwatch = Stopwatch.StartNew();
        var queryCount = 0;
        var results = new List<object>();

        if (showProblem)
        {
            // N+1 Problem: Fetch authors, then fetch company for each
            var authors = await _authorRepo.ReadAllAsync().Take(10).ToListAsync();
            queryCount++; // 1 query for authors

            foreach (var author in authors)
            {
                if (author.CompanyId.HasValue)
                {
                    var company = await _companyRepo.ReadByIdAsync(author.CompanyId.Value);
                    queryCount++; // N queries for companies
                    results.Add(new
                    {
                        AuthorName = author.Name,
                        CompanyName = company?.Name ?? "Unknown"
                    });
                }
            }
            stopwatch.Stop();

            return Ok(new
            {
                Scenario = "N+1 Problem Demonstrated",
                Description = "Fetching authors then loading company for each one individually",
                TotalQueries = queryCount,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Results = results,
                Problem = "This results in 1 + N queries where N is the number of authors",
                Solution = "Call this endpoint with ?showProblem=false to see the optimized version"
            });
        }
        else
        {
            // Solution: Use Include to fetch related data in single query
            var authorsWithCompanies = _authorRepo.Query()
                .Include(a => a.Company)
                .Take(10)
                .Execute()
                .ToList();
            queryCount = 1; // Only 1 query with JOIN

            foreach (var author in authorsWithCompanies)
            {
                results.Add(new
                {
                    AuthorName = author.Name,
                    CompanyName = author.Company?.Name ?? "Unknown"
                });
            }
            stopwatch.Stop();

            return Ok(new
            {
                Scenario = "N+1 Problem Solved",
                Description = "Using Include to fetch authors with companies in single query",
                TotalQueries = queryCount,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Results = results,
                Solution = "Using Include() performs a JOIN and fetches all data in 1 query",
                PerformanceGain = "Reduced from N+1 queries to just 1 query"
            });
        }
    }

    [HttpGet("index-performance")]
    public async Task<IActionResult> IndexPerformance()
    {
        var results = new Dictionary<string, object>();

        // Test 1: Query on indexed column (id - primary key)
        var indexedStopwatch = Stopwatch.StartNew();
        var byId = await _personRepo.ReadByIdAsync(1);
        indexedStopwatch.Stop();

        results["IndexedColumnQuery"] = new
        {
            Query = "SELECT by Primary Key (indexed)",
            ExecutionTimeMs = indexedStopwatch.ElapsedMilliseconds,
            Description = "Primary keys are automatically indexed",
            Performance = "Excellent - O(log n)"
        };

        // Test 2: Query on non-indexed column
        var nonIndexedStopwatch = Stopwatch.StartNew();
        var byEmail = await _personRepo.ReadFirstOrDefaultAsync(p => p.Email == "john.smith@company.com");
        nonIndexedStopwatch.Stop();

        results["NonIndexedColumnQuery"] = new
        {
            Query = "SELECT by Email (likely non-indexed)",
            ExecutionTimeMs = nonIndexedStopwatch.ElapsedMilliseconds,
            Description = "Full table scan required",
            Performance = "Poor for large tables - O(n)",
            Recommendation = "Add index on frequently queried columns"
        };

        // Test 3: Range query performance
        var rangeStopwatch = Stopwatch.StartNew();
        var salaryRange = await _personRepo.ReadManyAsync(p => p.Salary >= 70000 && p.Salary <= 90000).ToListAsync();
        rangeStopwatch.Stop();

        results["RangeQuery"] = new
        {
            Query = "SELECT with range condition",
            ExecutionTimeMs = rangeStopwatch.ElapsedMilliseconds,
            RecordsFound = salaryRange.Count,
            Description = "Range queries benefit from indexes",
            Recommendation = "Index columns used in WHERE clauses with ranges"
        };

        // Test 4: Compound query
        var compoundStopwatch = Stopwatch.StartNew();
        var compound = await _personRepo.ReadManyAsync(p => p.Department == "Engineering" && p.Salary > 80000).ToListAsync();
        compoundStopwatch.Stop();

        results["CompoundQuery"] = new
        {
            Query = "SELECT with multiple conditions",
            ExecutionTimeMs = compoundStopwatch.ElapsedMilliseconds,
            RecordsFound = compound.Count,
            Description = "Multiple conditions in WHERE clause",
            Recommendation = "Consider composite indexes for frequently used combinations"
        };

        return Ok(new
        {
            Title = "Index Performance Demonstration",
            Results = results,
            IndexingBestPractices = new[]
            {
                "Index primary keys (automatic)",
                "Index foreign keys for JOIN operations",
                "Index columns used in WHERE clauses",
                "Index columns used in ORDER BY",
                "Consider composite indexes for multiple column queries",
                "Monitor and remove unused indexes",
                "Balance between query speed and insert/update performance"
            }
        });
    }

    [HttpGet("connection-pooling")]
    public async Task<IActionResult> ConnectionPoolingTest([FromQuery] int concurrentRequests = 10)
    {
        if (concurrentRequests < 1 || concurrentRequests > 50)
            return BadRequest("Concurrent requests must be between 1 and 50");

        var results = new ConcurrentBag<long>();
        
        // Test without connection pooling simulation (sequential)
        var sequentialStopwatch = Stopwatch.StartNew();
        for (int i = 0; i < concurrentRequests; i++)
        {
            var sw = Stopwatch.StartNew();
            var person = await _personRepo.ReadByIdAsync(i % 5 + 1);
            sw.Stop();
            results.Add(sw.ElapsedMilliseconds);
        }
        sequentialStopwatch.Stop();

        var sequentialResults = results.ToList();
        results.Clear();

        // Test with parallel execution (simulating connection pool benefits)
        var parallelStopwatch = Stopwatch.StartNew();
        var tasks = new List<Task>();
        
        for (int i = 0; i < concurrentRequests; i++)
        {
            var index = i;
            tasks.Add(Task.Run(async () =>
            {
                var sw = Stopwatch.StartNew();
                var person = await _personRepo.ReadByIdAsync(index % 5 + 1);
                sw.Stop();
                results.Add(sw.ElapsedMilliseconds);
            }));
        }
        
        await Task.WhenAll(tasks);
        parallelStopwatch.Stop();

        var parallelResults = results.ToList();

        return Ok(new
        {
            Title = "Connection Pooling Performance Test",
            ConcurrentRequests = concurrentRequests,
            Sequential = new
            {
                TotalTimeMs = sequentialStopwatch.ElapsedMilliseconds,
                AverageTimePerRequestMs = sequentialResults.Any() ? sequentialResults.Average() : 0,
                MaxTimeMs = sequentialResults.Any() ? sequentialResults.Max() : 0,
                MinTimeMs = sequentialResults.Any() ? sequentialResults.Min() : 0,
                Description = "Sequential execution - one connection at a time"
            },
            Parallel = new
            {
                TotalTimeMs = parallelStopwatch.ElapsedMilliseconds,
                AverageTimePerRequestMs = parallelResults.Any() ? parallelResults.Average() : 0,
                MaxTimeMs = parallelResults.Any() ? parallelResults.Max() : 0,
                MinTimeMs = parallelResults.Any() ? parallelResults.Min() : 0,
                Description = "Parallel execution - utilizing connection pool"
            },
            PerformanceGain = new
            {
                SpeedupFactor = Math.Round((double)sequentialStopwatch.ElapsedMilliseconds / parallelStopwatch.ElapsedMilliseconds, 2),
                TimeSavedMs = sequentialStopwatch.ElapsedMilliseconds - parallelStopwatch.ElapsedMilliseconds,
                PercentImprovement = Math.Round(((double)sequentialStopwatch.ElapsedMilliseconds - parallelStopwatch.ElapsedMilliseconds) / sequentialStopwatch.ElapsedMilliseconds * 100, 2)
            },
            ConnectionPoolingBenefits = new[]
            {
                "Reuses existing connections",
                "Reduces connection establishment overhead",
                "Enables parallel query execution",
                "Improves throughput for concurrent requests",
                "Reduces database server load"
            }
        });
    }

    [HttpGet("batch-vs-individual")]
    public async Task<IActionResult> BatchVsIndividualComparison([FromQuery] int operationCount = 100)
    {
        if (operationCount < 10 || operationCount > 1000)
            return BadRequest("Operation count must be between 10 and 1000");

        var results = new Dictionary<string, object>();

        // Generate test data
        var testPeople = GenerateTestPeople(operationCount);
        
        // Insert test data for update/delete operations
        var insertedPeople = await _personRepo.CreateManyAsync(testPeople);
        var insertedIds = insertedPeople.Select(p => p.Id).ToList();

        // Test 1: Individual Updates vs Batch Update
        var individualUpdateStopwatch = Stopwatch.StartNew();
        var updateCount = 0;
        foreach (var id in insertedIds.Take(operationCount / 2))
        {
            var person = await _personRepo.ReadByIdAsync(id);
            if (person != null)
            {
                person.Salary *= 1.1m;
                await _personRepo.UpdateAsync(person);
                updateCount++;
            }
        }
        individualUpdateStopwatch.Stop();

        var batchUpdateStopwatch = Stopwatch.StartNew();
        var batchUpdated = await _personRepo.BatchUpdateAsync(
            p => insertedIds.Skip(operationCount / 2).Contains(p.Id),
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

        results["UpdateComparison"] = new
        {
            IndividualUpdates = new
            {
                Method = "Update one by one",
                RecordsUpdated = updateCount,
                TimeMs = individualUpdateStopwatch.ElapsedMilliseconds,
                TimePerRecordMs = Math.Round((double)individualUpdateStopwatch.ElapsedMilliseconds / updateCount, 2)
            },
            BatchUpdate = new
            {
                Method = "BatchUpdateAsync",
                RecordsUpdated = batchUpdated,
                TimeMs = batchUpdateStopwatch.ElapsedMilliseconds,
                TimePerRecordMs = batchUpdated > 0 ? Math.Round((double)batchUpdateStopwatch.ElapsedMilliseconds / batchUpdated, 2) : 0
            },
            SpeedupFactor = individualUpdateStopwatch.ElapsedMilliseconds > 0 
                ? Math.Round((double)individualUpdateStopwatch.ElapsedMilliseconds / batchUpdateStopwatch.ElapsedMilliseconds, 2) 
                : 0
        };

        // Test 2: Individual Deletes vs Batch Delete
        var individualDeleteStopwatch = Stopwatch.StartNew();
        var deleteCount = 0;
        foreach (var id in insertedIds.Take(operationCount / 4))
        {
            await _personRepo.DeleteByIdAsync(id);
            deleteCount++;
        }
        individualDeleteStopwatch.Stop();

        var batchDeleteStopwatch = Stopwatch.StartNew();
        var batchDeleted = await _personRepo.BatchDeleteAsync(
            p => insertedIds.Skip(operationCount / 4).Contains(p.Id));
        batchDeleteStopwatch.Stop();

        results["DeleteComparison"] = new
        {
            IndividualDeletes = new
            {
                Method = "Delete one by one",
                RecordsDeleted = deleteCount,
                TimeMs = individualDeleteStopwatch.ElapsedMilliseconds,
                TimePerRecordMs = Math.Round((double)individualDeleteStopwatch.ElapsedMilliseconds / deleteCount, 2)
            },
            BatchDelete = new
            {
                Method = "BatchDeleteAsync",
                RecordsDeleted = batchDeleted,
                TimeMs = batchDeleteStopwatch.ElapsedMilliseconds,
                TimePerRecordMs = batchDeleted > 0 ? Math.Round((double)batchDeleteStopwatch.ElapsedMilliseconds / batchDeleted, 2) : 0
            },
            SpeedupFactor = individualDeleteStopwatch.ElapsedMilliseconds > 0
                ? Math.Round((double)individualDeleteStopwatch.ElapsedMilliseconds / batchDeleteStopwatch.ElapsedMilliseconds, 2)
                : 0
        };

        return Ok(new
        {
            Title = "Batch vs Individual Operations Performance",
            OperationCount = operationCount,
            Results = results,
            Recommendations = new[]
            {
                "Use batch operations for bulk updates/deletes",
                "Batch operations reduce database round trips",
                "Significant performance gains for large datasets",
                "Consider transaction overhead for very large batches",
                "Test with realistic data volumes for your use case"
            }
        });
    }

    // Helper methods
    private List<Person> GenerateTestPeople(int count)
    {
        var people = new List<Person>();
        var departments = new[] { "Engineering", "Sales", "Marketing", "HR" };
        var random = new Random();

        for (int i = 1; i <= count; i++)
        {
            people.Add(new Person
            {
                FirstName = $"PerfTest{i}",
                LastName = $"User{i}",
                Age = 25 + (i % 40),
                Email = $"perftest{i}@test.com",
                Department = departments[i % departments.Length],
                Salary = 50000 + (random.Next(50) * 1000)
            });
        }

        return people;
    }

    private async Task<MethodPerformance> TestIndividualInserts(List<Person> people)
    {
        var stopwatch = Stopwatch.StartNew();
        var count = 0;

        foreach (var person in people)
        {
            await _personRepo.CreateAsync(person);
            count++;
        }

        stopwatch.Stop();

        return new MethodPerformance
        {
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
            RecordsProcessed = count,
            RecordsPerSecond = count > 0 && stopwatch.ElapsedMilliseconds > 0 
                ? Math.Round(count / (stopwatch.ElapsedMilliseconds / 1000.0), 2) 
                : 0,
            Notes = "Individual CreateAsync calls"
        };
    }

    private async Task<MethodPerformance> TestIndividualInsertsWithTransaction(List<Person> people)
    {
        var stopwatch = Stopwatch.StartNew();
        var count = 0;

        var transaction = await _personRepo.BeginTransactionAsync();
        try
        {
            foreach (var person in people)
            {
                await _personRepo.CreateAsync(person, transaction);
                count++;
            }
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }

        stopwatch.Stop();

        return new MethodPerformance
        {
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
            RecordsProcessed = count,
            RecordsPerSecond = count > 0 && stopwatch.ElapsedMilliseconds > 0
                ? Math.Round(count / (stopwatch.ElapsedMilliseconds / 1000.0), 2)
                : 0,
            Notes = "Individual inserts wrapped in transaction"
        };
    }

    private async Task<MethodPerformance> TestBulkInsert(List<Person> people)
    {
        var stopwatch = Stopwatch.StartNew();
        
        var created = await _personRepo.CreateManyAsync(people);
        var count = created.Count();

        stopwatch.Stop();

        return new MethodPerformance
        {
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
            RecordsProcessed = count,
            RecordsPerSecond = count > 0 && stopwatch.ElapsedMilliseconds > 0
                ? Math.Round(count / (stopwatch.ElapsedMilliseconds / 1000.0), 2)
                : 0,
            Notes = "CreateManyAsync bulk operation"
        };
    }

    private async Task<MethodPerformance> TestBulkInsertWithTransaction(List<Person> people)
    {
        var stopwatch = Stopwatch.StartNew();
        var count = 0;

        var transaction = await _personRepo.BeginTransactionAsync();
        try
        {
            var created = await _personRepo.CreateManyAsync(people, transaction);
            count = created.Count();
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }

        stopwatch.Stop();

        return new MethodPerformance
        {
            ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
            RecordsProcessed = count,
            RecordsPerSecond = count > 0 && stopwatch.ElapsedMilliseconds > 0
                ? Math.Round(count / (stopwatch.ElapsedMilliseconds / 1000.0), 2)
                : 0,
            Notes = "CreateManyAsync with transaction"
        };
    }
}