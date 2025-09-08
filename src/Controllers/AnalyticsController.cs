using Microsoft.AspNetCore.Mvc;
using Durable;
using Test.Shared;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AnalyticsController : ControllerBase
{
    private readonly IRepository<Person> _personRepo;

    public AnalyticsController(IRepository<Person> personRepo)
    {
        _personRepo = personRepo;
    }

    public class SalaryStats
    {
        public string Department { get; set; }
        public decimal AverageSalary { get; set; }
        public decimal MinimumSalary { get; set; }
        public decimal MaximumSalary { get; set; }
        public int EmployeeCount { get; set; }
    }

    public class OverallSalaryStats
    {
        public decimal AverageSalary { get; set; }
        public decimal MinimumSalary { get; set; }
        public decimal MaximumSalary { get; set; }
        public int TotalEmployees { get; set; }
        public List<SalaryStats> DepartmentBreakdown { get; set; } = new List<SalaryStats>();
    }

    [HttpGet("salary-by-department")]
    public async Task<IActionResult> GetSalaryStatsByDepartment()
    {
        try
        {
            // Get all people to group by department
            var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
            
            // Group by department and calculate statistics
            var departmentStats = allPeople
                .GroupBy(p => p.Department ?? "Unknown")
                .Select(group => new SalaryStats
                {
                    Department = group.Key,
                    AverageSalary = group.Average(p => p.Salary),
                    MinimumSalary = group.Min(p => p.Salary),
                    MaximumSalary = group.Max(p => p.Salary),
                    EmployeeCount = group.Count()
                })
                .OrderBy(s => s.Department)
                .ToList();

            return Ok(departmentStats);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve salary statistics by department", details = ex.Message });
        }
    }

    [HttpGet("salary-stats/{department}")]
    public async Task<IActionResult> GetDepartmentSalaryStats(string department)
    {
        if (string.IsNullOrWhiteSpace(department))
            return BadRequest("Department parameter is required");

        try
        {
            // Get people in the specific department
            var departmentPeople = await _personRepo.ReadManyAsync(p => p.Department == department).ToListAsync();
            
            if (!departmentPeople.Any())
                return NotFound($"No employees found in department: {department}");

            // Use ORM aggregate operations for the specific department
            var averageSalary = await _personRepo.AverageAsync(p => p.Salary, p => p.Department == department);
            var minimumSalary = await _personRepo.MinAsync(p => p.Salary, p => p.Department == department);
            var maximumSalary = await _personRepo.MaxAsync(p => p.Salary, p => p.Department == department);
            var employeeCount = await _personRepo.CountAsync(p => p.Department == department);

            var stats = new SalaryStats
            {
                Department = department,
                AverageSalary = averageSalary,
                MinimumSalary = minimumSalary,
                MaximumSalary = maximumSalary,
                EmployeeCount = employeeCount
            };

            return Ok(stats);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve department salary statistics", details = ex.Message });
        }
    }

    [HttpGet("overall-salary-stats")]
    public async Task<IActionResult> GetOverallSalaryStats()
    {
        try
        {
            // Use ORM aggregate operations for overall statistics
            var averageSalary = await _personRepo.AverageAsync(p => p.Salary);
            var minimumSalary = await _personRepo.MinAsync(p => p.Salary);
            var maximumSalary = await _personRepo.MaxAsync(p => p.Salary);
            var totalEmployees = await _personRepo.CountAsync();

            // Get department breakdown
            var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
            var departmentBreakdown = allPeople
                .GroupBy(p => p.Department ?? "Unknown")
                .Select(group => new SalaryStats
                {
                    Department = group.Key,
                    AverageSalary = group.Average(p => p.Salary),
                    MinimumSalary = group.Min(p => p.Salary),
                    MaximumSalary = group.Max(p => p.Salary),
                    EmployeeCount = group.Count()
                })
                .OrderBy(s => s.Department)
                .ToList();

            var overallStats = new OverallSalaryStats
            {
                AverageSalary = averageSalary,
                MinimumSalary = minimumSalary,
                MaximumSalary = maximumSalary,
                TotalEmployees = totalEmployees,
                DepartmentBreakdown = departmentBreakdown
            };

            return Ok(overallStats);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve overall salary statistics", details = ex.Message });
        }
    }

    [HttpGet("salary-ranges")]
    public async Task<IActionResult> GetSalaryRanges()
    {
        try
        {
            // Get all people and categorize by salary ranges
            var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
            
            var salaryRanges = new
            {
                Under50k = allPeople.Count(p => p.Salary < 50000),
                Range50kTo75k = allPeople.Count(p => p.Salary >= 50000 && p.Salary < 75000),
                Range75kTo100k = allPeople.Count(p => p.Salary >= 75000 && p.Salary < 100000),
                Over100k = allPeople.Count(p => p.Salary >= 100000),
                TotalEmployees = allPeople.Count,
                Ranges = new[]
                {
                    new { Range = "Under $50k", Count = allPeople.Count(p => p.Salary < 50000) },
                    new { Range = "$50k - $75k", Count = allPeople.Count(p => p.Salary >= 50000 && p.Salary < 75000) },
                    new { Range = "$75k - $100k", Count = allPeople.Count(p => p.Salary >= 75000 && p.Salary < 100000) },
                    new { Range = "Over $100k", Count = allPeople.Count(p => p.Salary >= 100000) }
                }
            };

            return Ok(salaryRanges);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve salary ranges", details = ex.Message });
        }
    }

    [HttpGet("departments")]
    public async Task<IActionResult> GetDepartments()
    {
        try
        {
            // Get distinct departments
            var allPeople = await _personRepo.ReadAllAsync().ToListAsync();
            var departments = allPeople
                .Select(p => p.Department ?? "Unknown")
                .Distinct()
                .OrderBy(d => d)
                .ToList();

            return Ok(departments);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = "Failed to retrieve departments", details = ex.Message });
        }
    }
}