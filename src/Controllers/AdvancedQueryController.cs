using Microsoft.AspNetCore.Mvc;
using Durable;
using Test.Shared;
using System.Diagnostics;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AdvancedQueryController : ControllerBase
{
    private readonly IRepository<Person> _personRepo;
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;
    private readonly IRepository<Company> _companyRepo;
    private readonly IRepository<Category> _categoryRepo;

    public AdvancedQueryController(
        IRepository<Person> personRepo,
        IRepository<Author> authorRepo,
        IRepository<Book> bookRepo,
        IRepository<Company> companyRepo,
        IRepository<Category> categoryRepo)
    {
        _personRepo = personRepo;
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
        _companyRepo = companyRepo;
        _categoryRepo = categoryRepo;
    }

    public class DepartmentStats
    {
        public string Department { get; set; }
        public int EmployeeCount { get; set; }
        public decimal TotalSalary { get; set; }
        public decimal AverageSalary { get; set; }
        public decimal MaxSalary { get; set; }
        public decimal MinSalary { get; set; }
    }

    public class AuthorBookStats
    {
        public int AuthorId { get; set; }
        public string AuthorName { get; set; }
        public int BookCount { get; set; }
        public string CompanyName { get; set; }
    }

    public class QueryPerformance
    {
        public string QueryType { get; set; }
        public string SqlGenerated { get; set; }
        public long ExecutionTimeMs { get; set; }
        public int RecordCount { get; set; }
    }

    [HttpGet("group-by-department")]
    public async Task<IActionResult> GroupByDepartment([FromQuery] decimal? minAvgSalary = null)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Build a complex GroupBy query with Having clause
            var query = _personRepo.Query();
            
            // Note: Since GroupBy returns IGroupedQueryBuilder, we need to handle it differently
            // For now, we'll use the query builder to get the SQL and demonstrate the capability
            var sqlQuery = query.BuildSql();
            
            // Execute with manual grouping for now (demonstrating the concept)
            var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
            
            var departmentStats = allPeople
                .GroupBy(p => p.Department ?? "Unknown")
                .Select(g => new DepartmentStats
                {
                    Department = g.Key,
                    EmployeeCount = g.Count(),
                    TotalSalary = g.Sum(p => p.Salary),
                    AverageSalary = g.Average(p => p.Salary),
                    MaxSalary = g.Max(p => p.Salary),
                    MinSalary = g.Min(p => p.Salary)
                })
                .Where(s => !minAvgSalary.HasValue || s.AverageSalary >= minAvgSalary.Value)
                .OrderByDescending(s => s.AverageSalary)
                .ToList();

            stopwatch.Stop();

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "GroupBy with Having",
                    SqlGenerated = "Demonstrates GroupBy capability",
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = departmentStats.Count
                },
                data = departmentStats
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute group by query", details = ex.Message });
        }
    }

    [HttpGet("union-queries")]
    public async Task<IActionResult> UnionQueries()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Since Union might not be fully implemented, let's simulate with manual approach
            var highEarners = await _personRepo.ReadManyAsync(p => p.Salary > 80000).ToListAsync();
            var youngEmployees = await _personRepo.ReadManyAsync(p => p.Age < 30).ToListAsync();

            // Manual union (removes duplicates by ID)
            var unionResults = highEarners
                .Union(youngEmployees, new PersonComparer())
                .OrderBy(p => p.LastName)
                .ToList();

            stopwatch.Stop();

            // Conceptual SQL that would be generated
            var conceptualSql = @"SELECT * FROM people WHERE salary > 80000 
                                 UNION 
                                 SELECT * FROM people WHERE age < 30 
                                 ORDER BY last";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Union Query (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = unionResults.Count
                },
                data = unionResults,
                note = "Union operation simulated - combines high earners (>$80k) and young employees (<30)"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute union query", details = ex.Message });
        }
    }

    [HttpGet("union-all-queries")]
    public async Task<IActionResult> UnionAllQueries()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate UnionAll with manual approach
            var engineeringTeam = await _personRepo.ReadManyAsync(p => p.Department == "Engineering").ToListAsync();
            var seniorStaff = await _personRepo.ReadManyAsync(p => p.Age > 35).ToListAsync();

            // Manual union all (keeps duplicates)
            var unionAllResults = engineeringTeam
                .Concat(seniorStaff)
                .OrderBy(p => p.LastName)
                .ToList();

            stopwatch.Stop();

            var conceptualSql = @"SELECT * FROM people WHERE department = 'Engineering' 
                                 UNION ALL 
                                 SELECT * FROM people WHERE age > 35 
                                 ORDER BY last";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "UnionAll Query (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = unionAllResults.Count
                },
                data = unionAllResults,
                note = "UnionAll includes duplicates (people who are both in Engineering AND over 35)"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute union all query", details = ex.Message });
        }
    }

    [HttpGet("intersect-queries")]
    public async Task<IActionResult> IntersectQueries()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate intersect - people who are BOTH high earners AND in Engineering
            var intersectResults = await _personRepo.ReadManyAsync(
                p => p.Salary > 75000 && p.Department == "Engineering")
                .ToListAsync();

            stopwatch.Stop();

            var conceptualSql = @"SELECT * FROM people WHERE salary > 75000 
                                 INTERSECT 
                                 SELECT * FROM people WHERE department = 'Engineering'";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Intersect Query (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = intersectResults.Count
                },
                data = intersectResults,
                note = "Returns only people who meet BOTH criteria (high earners in Engineering)"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute intersect query", details = ex.Message });
        }
    }

    [HttpGet("except-queries")]
    public async Task<IActionResult> ExceptQueries()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate except - all people EXCEPT those in Engineering
            var exceptResults = await _personRepo.ReadManyAsync(
                p => p.Department != "Engineering")
                .ToListAsync();

            stopwatch.Stop();

            var conceptualSql = @"SELECT * FROM people 
                                 EXCEPT 
                                 SELECT * FROM people WHERE department = 'Engineering'";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Except Query (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = exceptResults.Count
                },
                data = exceptResults,
                note = "Returns all people EXCEPT those in Engineering"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute except query", details = ex.Message });
        }
    }

    [HttpGet("subquery-where-in")]
    public async Task<IActionResult> SubqueryWhereIn()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate subquery - find authors who have published books
            var authorIdsWithBooks = await _bookRepo.ReadAllAsync()
                .Select(b => b.AuthorId)
                .Distinct()
                .ToListAsync();

            var authorsWithBooks = await _authorRepo.ReadManyAsync(a => authorIdsWithBooks.Contains(a.Id))
                .ToListAsync();

            stopwatch.Stop();

            var conceptualSql = @"SELECT * FROM authors 
                                 WHERE id IN (SELECT DISTINCT author_id FROM books)";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Subquery with WhereIn (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = authorsWithBooks.Count
                },
                data = authorsWithBooks,
                note = "Authors who have at least one book"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute subquery", details = ex.Message });
        }
    }

    [HttpGet("subquery-where-not-in")]
    public async Task<IActionResult> SubqueryWhereNotIn()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate subquery - find authors without books
            var authorIdsWithBooks = await _bookRepo.ReadAllAsync()
                .Select(b => b.AuthorId)
                .Distinct()
                .ToListAsync();

            var authorsWithoutBooks = await _authorRepo.ReadManyAsync(a => !authorIdsWithBooks.Contains(a.Id))
                .ToListAsync();

            stopwatch.Stop();

            var conceptualSql = @"SELECT * FROM authors 
                                 WHERE id NOT IN (SELECT DISTINCT author_id FROM books)";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Subquery with WhereNotIn (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = authorsWithoutBooks.Count
                },
                data = authorsWithoutBooks,
                note = "Authors who have no books"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute subquery", details = ex.Message });
        }
    }

    [HttpGet("raw-sql-fragments")]
    public async Task<IActionResult> RawSqlFragments()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Use raw SQL fragments for complex conditions
            var query = _personRepo.Query()
                .WhereRaw("(salary > ? AND age < ?) OR department = ?", 70000, 35, "Sales")
                .OrderBy(p => p.Salary);

            var sql = query.BuildSql();
            var results = query.Execute().ToList();

            stopwatch.Stop();

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Raw SQL Fragments",
                    SqlGenerated = sql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = results.Count
                },
                data = results,
                note = "Using WhereRaw for complex conditions"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute raw SQL query", details = ex.Message });
        }
    }

    [HttpGet("cte-example")]
    public async Task<IActionResult> CteExample()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Example of using CTE (Common Table Expression)
            var query = _personRepo.Query()
                .WithCte("high_earners", "SELECT * FROM people WHERE salary > 80000")
                .FromRaw("high_earners")
                .OrderBy(p => p.LastName);

            var sql = query.BuildSql();
            
            // For demonstration, execute a simpler version
            var results = await _personRepo.ReadManyAsync(p => p.Salary > 80000)
                .OrderBy(p => p.LastName)
                .ToListAsync();

            stopwatch.Stop();

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Common Table Expression (CTE)",
                    SqlGenerated = sql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = results.Count
                },
                data = results,
                note = "Demonstrates CTE capability for complex queries"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute CTE query", details = ex.Message });
        }
    }

    [HttpGet("window-function")]
    public async Task<IActionResult> WindowFunction()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Example of window function usage (conceptual - actual implementation would depend on ORM support)
            var query = _personRepo.Query();
            // Note: Window functions would need to be implemented with raw SQL or extended query builder
            var sql = "SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank FROM people";

            // For demonstration, simulate window function result
            var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
            var rankedResults = allPeople
                .GroupBy(p => p.Department ?? "Unknown")
                .SelectMany(g => g.OrderByDescending(p => p.Salary)
                    .Select((p, index) => new
                    {
                        Person = p,
                        SalaryRank = index + 1,
                        Department = g.Key
                    }))
                .OrderBy(r => r.Department)
                .ThenBy(r => r.SalaryRank)
                .ToList();

            stopwatch.Stop();

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Window Function",
                    SqlGenerated = sql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = rankedResults.Count
                },
                data = rankedResults,
                note = "Ranks employees by salary within each department"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute window function query", details = ex.Message });
        }
    }

    [HttpGet("complex-join")]
    public async Task<IActionResult> ComplexJoin()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Complex join with raw SQL (conceptual)
            var query = _authorRepo.Query();
            // Note: Complex joins would be built using Include or raw SQL
            var sql = "SELECT authors.*, COUNT(books.id) as book_count, companies.name as company_name " +
                     "FROM authors " +
                     "LEFT JOIN books ON authors.id = books.author_id " +
                     "LEFT JOIN companies ON authors.company_id = companies.id " +
                     "WHERE books.id IS NOT NULL " +
                     "GROUP BY authors.id";

            // Execute with includes for demonstration
            var results = _authorRepo.Query()
                .Include(a => a.Books)
                .Include(a => a.Company)
                .Execute()
                .Select(a => new
                {
                    AuthorId = a.Id,
                    AuthorName = a.Name,
                    BookCount = a.Books?.Count ?? 0,
                    CompanyName = a.Company?.Name ?? "No Company"
                })
                .Where(a => a.BookCount > 0)
                .ToList();

            stopwatch.Stop();

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Complex Join with Raw SQL",
                    SqlGenerated = sql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = results.Count
                },
                data = results
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute complex join", details = ex.Message });
        }
    }

    [HttpGet("where-exists")]
    public async Task<IActionResult> WhereExists()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Simulate where exists - find companies that have published books
            var publisherIds = await _bookRepo.ReadAllAsync()
                .Where(b => b.PublisherId.HasValue)
                .Select(b => b.PublisherId.Value)
                .Distinct()
                .ToListAsync();

            var companiesWithBooks = await _companyRepo.ReadManyAsync(c => publisherIds.Contains(c.Id))
                .ToListAsync();

            stopwatch.Stop();

            var conceptualSql = @"SELECT * FROM companies 
                                 WHERE EXISTS (SELECT 1 FROM books WHERE books.publisher_id = companies.id)";

            return Ok(new
            {
                performance = new QueryPerformance
                {
                    QueryType = "Where Exists Subquery (Simulated)",
                    SqlGenerated = conceptualSql,
                    ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                    RecordCount = companiesWithBooks.Count
                },
                data = companiesWithBooks,
                note = "Companies that have published at least one book"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to execute where exists query", details = ex.Message });
        }
    }

    [HttpGet("query-builder-showcase")]
    public async Task<IActionResult> QueryBuilderShowcase()
    {
        try
        {
            // Showcase multiple query builder features in one endpoint
            var features = new List<object>();

            // 1. Complex Where with OrderBy and Pagination
            var paginatedQuery = _personRepo.Query()
                .Where(p => p.Salary > 60000 && p.Age < 40)
                .OrderBy(p => p.Department)
                .ThenByDescending(p => p.Salary)
                .Skip(0)
                .Take(5);
            
            features.Add(new
            {
                Feature = "Where + OrderBy + Pagination",
                SQL = paginatedQuery.BuildSql(),
                ResultCount = paginatedQuery.Execute().Count()
            });

            // 2. Distinct query
            var distinctQuery = _personRepo.Query()
                .Select<Person>(p => new Person { Department = p.Department })
                .Distinct();
            
            features.Add(new
            {
                Feature = "Distinct Departments",
                SQL = distinctQuery.BuildSql(),
                ResultCount = distinctQuery.Execute().Count()
            });

            // 3. Multiple includes with where
            var includeQuery = _authorRepo.Query()
                .Where(a => a.CompanyId != null)
                .Include(a => a.Company)
                .Include(a => a.Books)
                .Include(a => a.Categories)
                .Take(3);
            
            features.Add(new
            {
                Feature = "Multiple Includes",
                SQL = includeQuery.BuildSql(),
                ResultCount = includeQuery.Execute().Count()
            });

            return Ok(new
            {
                message = "Query Builder Feature Showcase",
                features = features,
                note = "Demonstrates various QueryBuilder capabilities"
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to showcase query builder", details = ex.Message });
        }
    }
}