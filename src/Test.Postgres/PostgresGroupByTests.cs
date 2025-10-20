namespace Test.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Npgsql;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Comprehensive tests for GROUP BY operations including HAVING clauses, aggregate functions,
    /// and complex statistical reporting functionality in PostgreSQL.
    /// </summary>
    public class PostgresGroupByTests : IDisposable
    {
        #region Private-Members

        private const string TestConnectionString = "Host=localhost;Database=durable_groupby_test;Username=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_groupby_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        private readonly PostgresRepository<Person> _PersonRepository = null!;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the GROUP BY tests by setting up the test database and repository.
        /// </summary>
        public PostgresGroupByTests()
        {
            lock (_TestLock)
            {
                if (!_DatabaseSetupComplete)
                {
                    SetupTestDatabase();
                    _DatabaseSetupComplete = true;
                }
            }

            if (_SkipTests) return;

            // Initialize repository
            _PersonRepository = new PostgresRepository<Person>(TestConnectionString);

            // Create tables and insert test data
            CreateTables();
            InsertTestData();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests simple GROUP BY with Count aggregate function.
        /// </summary>
        [Fact]
        public async Task TestSimpleGroupByWithCount()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing simple GROUP BY with Count...");

            IEnumerable<IGrouping<string, Person>> groupsByDept = await _PersonRepository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = groupsByDept.ToList();

            Assert.True(groupList.Count >= 4); // Should have IT, HR, Sales, Finance
            Console.WriteLine($"Found {groupList.Count} departments");

            foreach (IGrouping<string, Person> group in groupList)
            {
                int count = group.Count();
                Console.WriteLine($"  Department: {group.Key}, Count: {count}");
                Assert.True(count > 0);

                foreach (Person person in group)
                {
                    Console.WriteLine($"    - {person.FirstName} {person.LastName}");
                    Assert.Equal(group.Key, person.Department);
                }
            }

            Console.WriteLine("✅ Simple GROUP BY with Count test passed!");
        }

        /// <summary>
        /// Tests GROUP BY with WHERE clause filtering before grouping.
        /// </summary>
        [Fact]
        public async Task TestGroupByWithWhere()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing GROUP BY with WHERE clause...");

            IEnumerable<IGrouping<string, Person>> filteredGroups = await _PersonRepository.Query()
                .Where(p => p.Age > 30)
                .GroupBy(p => p.Department)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = filteredGroups.ToList();

            Assert.True(groupList.Count >= 2);
            Console.WriteLine($"Found {groupList.Count} departments with people over 30");

            foreach (IGrouping<string, Person> group in groupList)
            {
                int count = group.Count();
                Console.WriteLine($"  Department: {group.Key}, Count: {count}");

                foreach (Person person in group)
                {
                    Assert.True(person.Age > 30);
                    Console.WriteLine($"    - {person.FirstName} {person.LastName}, Age: {person.Age}");
                }
            }

            Console.WriteLine("✅ GROUP BY with WHERE test passed!");
        }

        /// <summary>
        /// Tests GROUP BY with simple HAVING clause.
        /// </summary>
        [Fact]
        public async Task TestGroupByWithSimpleHaving()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing GROUP BY with simple HAVING clause...");

            IEnumerable<IGrouping<string, Person>> havingGroups = await _PersonRepository.Query()
                .GroupBy(p => p.Department)
                .Having(g => g.Count() > 2)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = havingGroups.ToList();

            Console.WriteLine($"Found {groupList.Count} departments with more than 2 people");

            foreach (IGrouping<string, Person> group in groupList)
            {
                int count = group.Count();
                Console.WriteLine($"  Department: {group.Key}, Count: {count}");
                Assert.True(count > 2);
            }

            Console.WriteLine("✅ GROUP BY with simple HAVING test passed!");
        }

        /// <summary>
        /// Tests GROUP BY with complex HAVING clause using multiple aggregate conditions.
        /// </summary>
        [Fact]
        public async Task TestGroupByWithComplexHaving()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing GROUP BY with complex HAVING clause...");

            IEnumerable<IGrouping<string, Person>> complexGroups = await _PersonRepository.Query()
                .GroupBy(p => p.Department)
                .Having(g => g.Count() > 1 && g.Average(p => p.Salary) > 60000)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = complexGroups.ToList();

            Console.WriteLine($"Found {groupList.Count} departments with >1 person AND average salary >$60,000");

            foreach (IGrouping<string, Person> group in groupList)
            {
                int count = group.Count();
                decimal avgSalary = group.Average(p => p.Salary);
                Console.WriteLine($"  Department: {group.Key}, Count: {count}, Avg Salary: ${avgSalary:N2}");

                Assert.True(count > 1);
                Assert.True(avgSalary > 60000);
            }

            Console.WriteLine("✅ GROUP BY with complex HAVING test passed!");
        }

        /// <summary>
        /// Tests all aggregate functions: Sum, Average, Min, Max, Count.
        /// </summary>
        [Fact]
        public async Task TestAllAggregateFunctions()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing all aggregate functions...");

            // Test Sum aggregate
            Console.WriteLine("\n--- Sum Aggregate ---");
            IGroupedQueryBuilder<Person, string> salaryByDept = _PersonRepository.Query()
                .GroupBy(p => p.Department);

            decimal totalSalary = await salaryByDept.SumAsync(p => p.Salary);
            Console.WriteLine($"Total salary across all groups: ${totalSalary:N2}");
            Assert.True(totalSalary > 0);

            // Test detailed aggregates by department
            IEnumerable<IGrouping<string, Person>> deptGroups = await _PersonRepository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = deptGroups.ToList();

            Console.WriteLine("\n--- All Aggregates by Department ---");
            foreach (IGrouping<string, Person> group in groupList)
            {
                int count = group.Count();
                decimal sum = group.Sum(p => p.Salary);
                decimal avg = group.Average(p => p.Salary);
                decimal min = group.Min(p => p.Salary);
                decimal max = group.Max(p => p.Salary);
                int minAge = group.Min(p => p.Age);
                int maxAge = group.Max(p => p.Age);

                Console.WriteLine($"  Department: {group.Key}");
                Console.WriteLine($"    Count: {count}");
                Console.WriteLine($"    Total Salary: ${sum:N2}");
                Console.WriteLine($"    Average Salary: ${avg:N2}");
                Console.WriteLine($"    Min Salary: ${min:N2}");
                Console.WriteLine($"    Max Salary: ${max:N2}");
                Console.WriteLine($"    Age Range: {minAge} - {maxAge}");

                // Verify aggregates
                Assert.True(count > 0);
                Assert.True(sum > 0);
                Assert.True(avg > 0);
                Assert.True(min > 0);
                Assert.True(max >= min);
                Assert.True(minAge > 0);
                Assert.True(maxAge >= minAge);
            }

            Console.WriteLine("✅ All aggregate functions test passed!");
        }

        /// <summary>
        /// Tests async aggregate methods on grouped queries.
        /// </summary>
        [Fact]
        public async Task TestAsyncAggregateMethods()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing async aggregate methods...");

            // Test async aggregates on IT department
            IGroupedQueryBuilder<Person, string> itGroup = _PersonRepository.Query()
                .Where(p => p.Department == "IT")
                .GroupBy(p => p.Department);

            int itCount = await itGroup.CountAsync();
            decimal itSum = await itGroup.SumAsync(p => p.Salary);
            decimal itAvg = await itGroup.AverageAsync(p => p.Salary);
            decimal itMax = await itGroup.MaxAsync(p => p.Salary);
            decimal itMin = await itGroup.MinAsync(p => p.Salary);

            Console.WriteLine($"IT Department Statistics (Async):");
            Console.WriteLine($"  Count: {itCount}");
            Console.WriteLine($"  Total Salary: ${itSum:N2}");
            Console.WriteLine($"  Average Salary: ${itAvg:N2}");
            Console.WriteLine($"  Max Salary: ${itMax:N2}");
            Console.WriteLine($"  Min Salary: ${itMin:N2}");

            // Verify async results
            Assert.True(itCount > 0);
            Assert.True(itSum > 0);
            Assert.True(itAvg > 0);
            Assert.True(itMax >= itMin);

            // Test async aggregates on all departments
            IGroupedQueryBuilder<Person, string> allDepts = _PersonRepository.Query()
                .GroupBy(p => p.Department);

            int totalCount = await allDepts.CountAsync();
            decimal totalSum = await allDepts.SumAsync(p => p.Salary);
            decimal overallAvg = await allDepts.AverageAsync(p => p.Salary);

            Console.WriteLine($"\nOverall Statistics (Async):");
            Console.WriteLine($"  Total Count: {totalCount}");
            Console.WriteLine($"  Total Sum: ${totalSum:N2}");
            Console.WriteLine($"  Overall Average: ${overallAvg:N2}");

            Assert.True(totalCount >= itCount);
            Assert.True(totalSum >= itSum);
            Assert.True(overallAvg > 0);

            Console.WriteLine("✅ Async aggregate methods test passed!");
        }

        /// <summary>
        /// Tests complex multi-aggregate statistics generation and reporting.
        /// </summary>
        [Fact]
        public async Task TestComplexStatisticsGeneration()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing complex statistics generation...");

            // Generate comprehensive department statistics
            IEnumerable<IGrouping<string, Person>> deptStats = await _PersonRepository.Query()
                .GroupBy(p => p.Department)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = deptStats.ToList();

            Console.WriteLine("\n=== COMPREHENSIVE DEPARTMENT STATISTICS ===");

            var departmentReport = new List<DepartmentStatistics>();

            foreach (IGrouping<string, Person> group in groupList)
            {
                var stats = new DepartmentStatistics
                {
                    Department = group.Key,
                    EmployeeCount = group.Count(),
                    TotalSalary = group.Sum(p => p.Salary),
                    AverageSalary = group.Average(p => p.Salary),
                    MinSalary = group.Min(p => p.Salary),
                    MaxSalary = group.Max(p => p.Salary),
                    SalaryRange = group.Max(p => p.Salary) - group.Min(p => p.Salary),
                    AverageAge = group.Average(p => p.Age),
                    MinAge = group.Min(p => p.Age),
                    MaxAge = group.Max(p => p.Age),
                    SeniorEmployees = group.Count(p => p.Age >= 40),
                    HighEarners = group.Count(p => p.Salary >= 80000)
                };

                departmentReport.Add(stats);

                Console.WriteLine($"\nDepartment: {stats.Department}");
                Console.WriteLine($"  Employees: {stats.EmployeeCount}");
                Console.WriteLine($"  Total Payroll: ${stats.TotalSalary:N2}");
                Console.WriteLine($"  Average Salary: ${stats.AverageSalary:N2}");
                Console.WriteLine($"  Salary Range: ${stats.MinSalary:N2} - ${stats.MaxSalary:N2} (Range: ${stats.SalaryRange:N2})");
                Console.WriteLine($"  Average Age: {stats.AverageAge:F1}");
                Console.WriteLine($"  Age Range: {stats.MinAge} - {stats.MaxAge}");
                Console.WriteLine($"  Senior Employees (40+): {stats.SeniorEmployees}");
                Console.WriteLine($"  High Earners ($80K+): {stats.HighEarners}");

                // Verify statistics
                Assert.True(stats.EmployeeCount > 0);
                Assert.True(stats.TotalSalary > 0);
                Assert.True(stats.AverageSalary > 0);
                Assert.True(stats.MaxSalary >= stats.MinSalary);
                Assert.True(stats.SalaryRange >= 0);
                Assert.True(stats.MaxAge >= stats.MinAge);
                Assert.True(stats.SeniorEmployees <= stats.EmployeeCount);
                Assert.True(stats.HighEarners <= stats.EmployeeCount);
            }

            // Generate company-wide statistics
            Console.WriteLine("\n=== COMPANY-WIDE STATISTICS ===");
            var companyStats = new
            {
                TotalEmployees = departmentReport.Sum(d => d.EmployeeCount),
                TotalPayroll = departmentReport.Sum(d => d.TotalSalary),
                AverageCompanySalary = departmentReport.Sum(d => d.TotalSalary) / departmentReport.Sum(d => d.EmployeeCount),
                DepartmentCount = departmentReport.Count,
                HighestPaidDepartment = departmentReport.OrderByDescending(d => d.AverageSalary).First(),
                LargestDepartment = departmentReport.OrderByDescending(d => d.EmployeeCount).First(),
                TotalSeniorEmployees = departmentReport.Sum(d => d.SeniorEmployees),
                TotalHighEarners = departmentReport.Sum(d => d.HighEarners)
            };

            Console.WriteLine($"Total Employees: {companyStats.TotalEmployees}");
            Console.WriteLine($"Total Payroll: ${companyStats.TotalPayroll:N2}");
            Console.WriteLine($"Average Company Salary: ${companyStats.AverageCompanySalary:N2}");
            Console.WriteLine($"Number of Departments: {companyStats.DepartmentCount}");
            Console.WriteLine($"Highest Paid Department: {companyStats.HighestPaidDepartment.Department} (${companyStats.HighestPaidDepartment.AverageSalary:N2})");
            Console.WriteLine($"Largest Department: {companyStats.LargestDepartment.Department} ({companyStats.LargestDepartment.EmployeeCount} employees)");
            Console.WriteLine($"Total Senior Employees: {companyStats.TotalSeniorEmployees}");
            Console.WriteLine($"Total High Earners: {companyStats.TotalHighEarners}");

            // Verify company statistics
            Assert.True(companyStats.TotalEmployees >= 10); // We inserted 10+ people
            Assert.True(companyStats.TotalPayroll > 0);
            Assert.True(companyStats.AverageCompanySalary > 0);
            Assert.True(companyStats.DepartmentCount >= 4);
            Assert.NotNull(companyStats.HighestPaidDepartment);
            Assert.NotNull(companyStats.LargestDepartment);

            Console.WriteLine("✅ Complex statistics generation test passed!");
        }

        /// <summary>
        /// Tests GROUP BY with multiple levels of grouping criteria.
        /// </summary>
        [Fact]
        public async Task TestMultiLevelGrouping()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing multi-level grouping...");

            // Group by department and age range
            IEnumerable<IGrouping<object, Person>> multiGroups = await _PersonRepository.Query()
                .GroupBy(p => new { p.Department, AgeGroup = p.Age < 30 ? "Young" : p.Age < 50 ? "Middle" : "Senior" })
                .ExecuteAsync();

            List<IGrouping<object, Person>> groupList = multiGroups.ToList();

            Console.WriteLine($"Found {groupList.Count} department/age group combinations");

            foreach (var group in groupList)
            {
                int count = group.Count();
                decimal avgSalary = group.Average(p => p.Salary);
                Console.WriteLine($"  Group: {group.Key}, Count: {count}, Avg Salary: ${avgSalary:N2}");

                Assert.True(count > 0);
                Assert.True(avgSalary > 0);
            }

            Console.WriteLine("✅ Multi-level grouping test passed!");
        }

        /// <summary>
        /// Tests GROUP BY with HAVING clause using aggregate functions in conditions.
        /// </summary>
        [Fact]
        public async Task TestHavingWithAggregateConditions()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing HAVING with aggregate conditions...");

            // Find departments with specific salary characteristics
            IEnumerable<IGrouping<string, Person>> selectiveDepts = await _PersonRepository.Query()
                .GroupBy(p => p.Department)
                .Having(g => g.Average(p => p.Salary) > 55000 && g.Max(p => p.Salary) < 100000)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = selectiveDepts.ToList();

            Console.WriteLine($"Found {groupList.Count} departments with avg salary >$55K and max <$100K");

            foreach (IGrouping<string, Person> group in groupList)
            {
                decimal avgSalary = group.Average(p => p.Salary);
                decimal maxSalary = group.Max(p => p.Salary);
                int count = group.Count();

                Console.WriteLine($"  Department: {group.Key}");
                Console.WriteLine($"    Count: {count}, Avg: ${avgSalary:N2}, Max: ${maxSalary:N2}");

                Assert.True(avgSalary > 55000);
                Assert.True(maxSalary < 100000);
            }

            Console.WriteLine("✅ HAVING with aggregate conditions test passed!");
        }

        /// <summary>
        /// Tests SQL generation and capture for GROUP BY operations.
        /// </summary>
        [Fact]
        public async Task TestGroupBySqlGeneration()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing GROUP BY SQL generation...");

            _PersonRepository.CaptureSql = true;

            // Execute GROUP BY query with HAVING clause
            IEnumerable<IGrouping<string, Person>> groups = await _PersonRepository.Query()
                .Where(p => p.Age > 25)
                .GroupBy(p => p.Department)
                .Having(g => g.Count() > 1)
                .ExecuteAsync();

            List<IGrouping<string, Person>> groupList = groups.ToList(); // Force execution

            Assert.NotNull(_PersonRepository.LastExecutedSql);
            string sql = _PersonRepository.LastExecutedSql.ToUpper();

            Assert.Contains("GROUP BY", sql);
            Assert.Contains("HAVING", sql);
            Assert.Contains("WHERE", sql);
            Assert.Contains("COUNT", sql);

            Console.WriteLine($"Generated SQL: {_PersonRepository.LastExecutedSql}");
            Console.WriteLine($"Retrieved {groupList.Count} groups");

            Console.WriteLine("✅ GROUP BY SQL generation test passed!");
        }

        /// <summary>
        /// Disposes test resources.
        /// </summary>
        public void Dispose()
        {
            _PersonRepository?.Dispose();
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Sets up the test database by ensuring it exists and is accessible.
        /// </summary>
        private void SetupTestDatabase()
        {
            try
            {
                // Test connection to database
                using NpgsqlConnection connection = new NpgsqlConnection(TestConnectionString);
                connection.Open();

                using NpgsqlCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();

                Console.WriteLine("✅ PostgreSQL GROUP BY tests enabled - database connection successful");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  PostgreSQL GROUP BY tests disabled - could not connect to database: {ex.Message}");
                Console.WriteLine("To enable PostgreSQL GROUP BY tests:");
                Console.WriteLine("1. Start PostgreSQL server on localhost:5432");
                Console.WriteLine($"2. Create database: CREATE DATABASE {TestDatabaseName};");
                Console.WriteLine("3. Create user: CREATE USER test_user WITH PASSWORD 'test_password';");
                Console.WriteLine($"4. Grant permissions: GRANT ALL PRIVILEGES ON DATABASE {TestDatabaseName} TO test_user;");
                _SkipTests = true;
            }
        }

        /// <summary>
        /// Creates the people table for GROUP BY testing.
        /// </summary>
        private void CreateTables()
        {
            if (_SkipTests) return;

            // Drop table if exists
            try
            {
                _PersonRepository.ExecuteSql("DROP TABLE IF EXISTS people");
            }
            catch
            {
                // Table might not exist, ignore
            }

            // Create table with indexes for GROUP BY performance
            _PersonRepository.ExecuteSql(@"
                CREATE TABLE people (
                    id SERIAL PRIMARY KEY,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(128) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(32) NOT NULL
                )");

            _PersonRepository.ExecuteSql("CREATE INDEX idx_department ON people (department)");
            _PersonRepository.ExecuteSql("CREATE INDEX idx_age ON people (age)");
            _PersonRepository.ExecuteSql("CREATE INDEX idx_salary ON people (salary)");
            _PersonRepository.ExecuteSql("CREATE INDEX idx_dept_age ON people (department, age)");
            _PersonRepository.ExecuteSql("CREATE INDEX idx_dept_salary ON people (department, salary)");

            Console.WriteLine("✅ People table created with GROUP BY optimized indexes");
        }

        /// <summary>
        /// Inserts comprehensive test data for GROUP BY operations.
        /// </summary>
        private void InsertTestData()
        {
            if (_SkipTests) return;

            Console.WriteLine("Seeding test data for GROUP BY functionality...");

            var testPeople = new List<Person>
            {
                // IT Department (4 people)
                new Person { FirstName = "John", LastName = "Doe", Age = 30, Email = "john.doe@company.com", Salary = 75000, Department = "IT" },
                new Person { FirstName = "Jane", LastName = "Smith", Age = 28, Email = "jane.smith@company.com", Salary = 72000, Department = "IT" },
                new Person { FirstName = "David", LastName = "Johnson", Age = 35, Email = "david.johnson@company.com", Salary = 85000, Department = "IT" },
                new Person { FirstName = "Sarah", LastName = "Wilson", Age = 32, Email = "sarah.wilson@company.com", Salary = 78000, Department = "IT" },

                // HR Department (3 people)
                new Person { FirstName = "Michael", LastName = "Brown", Age = 42, Email = "michael.brown@company.com", Salary = 65000, Department = "HR" },
                new Person { FirstName = "Emily", LastName = "Davis", Age = 38, Email = "emily.davis@company.com", Salary = 68000, Department = "HR" },
                new Person { FirstName = "Robert", LastName = "Miller", Age = 45, Email = "robert.miller@company.com", Salary = 70000, Department = "HR" },

                // Sales Department (4 people)
                new Person { FirstName = "Lisa", LastName = "Garcia", Age = 29, Email = "lisa.garcia@company.com", Salary = 55000, Department = "Sales" },
                new Person { FirstName = "Mark", LastName = "Rodriguez", Age = 33, Email = "mark.rodriguez@company.com", Salary = 58000, Department = "Sales" },
                new Person { FirstName = "Amanda", LastName = "Martinez", Age = 27, Email = "amanda.martinez@company.com", Salary = 52000, Department = "Sales" },
                new Person { FirstName = "Kevin", LastName = "Lopez", Age = 31, Email = "kevin.lopez@company.com", Salary = 60000, Department = "Sales" },

                // Finance Department (3 people)
                new Person { FirstName = "Jennifer", LastName = "Anderson", Age = 36, Email = "jennifer.anderson@company.com", Salary = 82000, Department = "Finance" },
                new Person { FirstName = "Christopher", LastName = "Taylor", Age = 41, Email = "christopher.taylor@company.com", Salary = 88000, Department = "Finance" },
                new Person { FirstName = "Michelle", LastName = "Thomas", Age = 34, Email = "michelle.thomas@company.com", Salary = 80000, Department = "Finance" },

                // Marketing Department (2 people)
                new Person { FirstName = "Ryan", LastName = "Jackson", Age = 26, Email = "ryan.jackson@company.com", Salary = 48000, Department = "Marketing" },
                new Person { FirstName = "Nicole", LastName = "White", Age = 30, Email = "nicole.white@company.com", Salary = 52000, Department = "Marketing" }
            };

            foreach (Person person in testPeople)
            {
                _PersonRepository.Create(person);
            }

            Console.WriteLine("✅ Test data seeded successfully");
            Console.WriteLine($"   Total People: {testPeople.Count}");
            Console.WriteLine($"   Departments: IT (4), HR (3), Sales (4), Finance (3), Marketing (2)");
            Console.WriteLine($"   Age Range: 26-45, Salary Range: $48K-$88K");
        }

        #endregion

        #region Helper-Classes

        /// <summary>
        /// Helper class for comprehensive department statistics.
        /// </summary>
        private class DepartmentStatistics
        {
            /// <summary>
            /// Gets or sets the department name.
            /// </summary>
            public string Department { get; set; } = "";

            /// <summary>
            /// Gets or sets the total number of employees in the department.
            /// </summary>
            public int EmployeeCount { get; set; }

            /// <summary>
            /// Gets or sets the total salary for all employees in the department.
            /// </summary>
            public decimal TotalSalary { get; set; }

            /// <summary>
            /// Gets or sets the average salary across all employees in the department.
            /// </summary>
            public decimal AverageSalary { get; set; }

            /// <summary>
            /// Gets or sets the minimum salary in the department.
            /// </summary>
            public decimal MinSalary { get; set; }

            /// <summary>
            /// Gets or sets the maximum salary in the department.
            /// </summary>
            public decimal MaxSalary { get; set; }

            /// <summary>
            /// Gets or sets the salary range (difference between max and min salaries).
            /// </summary>
            public decimal SalaryRange { get; set; }

            /// <summary>
            /// Gets or sets the average age of employees in the department.
            /// </summary>
            public double AverageAge { get; set; }

            /// <summary>
            /// Gets or sets the minimum age in the department.
            /// </summary>
            public int MinAge { get; set; }

            /// <summary>
            /// Gets or sets the maximum age in the department.
            /// </summary>
            public int MaxAge { get; set; }

            /// <summary>
            /// Gets or sets the count of senior employees (age 40 and above).
            /// </summary>
            public int SeniorEmployees { get; set; }

            /// <summary>
            /// Gets or sets the count of high earners (salary $80,000 and above).
            /// </summary>
            public int HighEarners { get; set; }
        }

        #endregion
    }
}
