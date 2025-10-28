namespace Test.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Comprehensive tests for projection operations including type projections, anonymous types,
    /// member initialization, distinct operations, and pagination with custom result types.
    /// </summary>
    public class MySqlProjectionTests : IDisposable
    {
        #region Private-Members

        private const string TestConnectionString = "Server=localhost;Database=durable_projection_test;User=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_projection_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        private readonly MySqlRepository<Person> _PersonRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the projection tests by setting up the test database and repository.
        /// </summary>
        public MySqlProjectionTests()
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
            _PersonRepository = new MySqlRepository<Person>(TestConnectionString);

            // Create tables and insert test data
            CreateTables();
            InsertTestData();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests basic type projection into a specific result class.
        /// </summary>
        [Fact]
        public async Task TestBasicTypeProjection()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing basic type projection...");

            // Project Person entities into PersonSummary objects
            IEnumerable<PersonSummary> summaries = await _PersonRepository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .ExecuteAsync();

            List<PersonSummary> summaryList = summaries.ToList();

            Assert.True(summaryList.Count >= 8);
            Console.WriteLine($"Retrieved {summaryList.Count} person summaries");

            foreach (PersonSummary summary in summaryList)
            {
                Console.WriteLine($"  {summary.FirstName} {summary.LastName} - {summary.Email} (${summary.Salary:N2})");

                // Verify projected data
                Assert.False(string.IsNullOrEmpty(summary.FirstName));
                Assert.False(string.IsNullOrEmpty(summary.LastName));
                Assert.False(string.IsNullOrEmpty(summary.Email));
                Assert.True(summary.Salary > 0);
            }

            Console.WriteLine("✅ Basic type projection test passed!");
        }

        /// <summary>
        /// Tests projection into department information objects.
        /// </summary>
        [Fact]
        public async Task TestDepartmentInfoProjection()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing department info projection...");

            // Project into DepartmentInfo with department and salary information
            IEnumerable<DepartmentInfo> deptInfo = await _PersonRepository.Query()
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary,
                    EmployeeName = p.FirstName + " " + p.LastName
                })
                .ExecuteAsync();

            List<DepartmentInfo> deptList = deptInfo.ToList();

            Assert.True(deptList.Count >= 8);
            Console.WriteLine($"Retrieved {deptList.Count} department info records");

            foreach (DepartmentInfo info in deptList)
            {
                Console.WriteLine($"  {info.Department}: {info.EmployeeName} - ${info.Salary:N2}");

                Assert.False(string.IsNullOrEmpty(info.Department));
                Assert.False(string.IsNullOrEmpty(info.EmployeeName));
                Assert.True(info.Salary > 0);
            }

            Console.WriteLine("✅ Department info projection test passed!");
        }

        /// <summary>
        /// Tests projection with member initialization and computed fields.
        /// </summary>
        [Fact]
        public async Task TestComplexMemberInitialization()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing complex member initialization...");

            // Project with computed fields and member initialization
            IEnumerable<EmployeeProfile> profiles = await _PersonRepository.Query()
                .Select(p => new EmployeeProfile
                {
                    FullName = p.FirstName + " " + p.LastName,
                    ContactInfo = p.Email,
                    WorkInfo = p.Department + " Department",
                    SalaryInfo = p.Salary,
                    AgeCategory = p.Age < 30 ? "Young" : p.Age < 50 ? "Experienced" : "Senior",
                    IsHighEarner = p.Salary >= 70000,
                    YearsToRetirement = 65 - p.Age
                })
                .ExecuteAsync();

            List<EmployeeProfile> profileList = profiles.ToList();

            Assert.True(profileList.Count >= 8);
            Console.WriteLine($"Retrieved {profileList.Count} employee profiles");

            foreach (EmployeeProfile profile in profileList)
            {
                Console.WriteLine($"  {profile.FullName} ({profile.AgeCategory})");
                Console.WriteLine($"    Contact: {profile.ContactInfo}");
                Console.WriteLine($"    Work: {profile.WorkInfo}");
                Console.WriteLine($"    Salary: ${profile.SalaryInfo:N2} (High Earner: {profile.IsHighEarner})");
                Console.WriteLine($"    Years to Retirement: {profile.YearsToRetirement}");

                // Verify computed fields
                Assert.False(string.IsNullOrEmpty(profile.FullName));
                Assert.Contains(" ", profile.FullName); // Should have space between names
                Assert.Contains("@", profile.ContactInfo); // Should be email
                Assert.Contains("Department", profile.WorkInfo);
                Assert.Contains(profile.AgeCategory, new[] { "Young", "Experienced", "Senior" });
            }

            Console.WriteLine("✅ Complex member initialization test passed!");
        }

        /// <summary>
        /// Tests projection with ordering and filtering.
        /// </summary>
        [Fact]
        public async Task TestProjectionWithOrdering()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing projection with ordering...");

            // Project with ordering by projected field
            IEnumerable<PersonSummary> orderedSummaries = await _PersonRepository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .OrderByDescending(s => s.Salary)
                .ThenBy(s => s.LastName)
                .ExecuteAsync();

            List<PersonSummary> summaryList = orderedSummaries.ToList();

            Assert.True(summaryList.Count >= 8);
            Console.WriteLine($"Retrieved {summaryList.Count} ordered person summaries");

            decimal previousSalary = decimal.MaxValue;
            string previousLastName = "";

            foreach (PersonSummary summary in summaryList)
            {
                Console.WriteLine($"  {summary.FirstName} {summary.LastName} - ${summary.Salary:N2}");

                // Verify ordering (salary descending, then last name ascending)
                if (summary.Salary == previousSalary)
                {
                    // If salaries are equal, last name should be in ascending order
                    if (!string.IsNullOrEmpty(previousLastName) && summary.Salary == previousSalary)
                    {
                        Assert.True(string.Compare(previousLastName, summary.LastName, StringComparison.Ordinal) <= 0);
                    }
                }
                else
                {
                    // Salary should be descending
                    Assert.True(summary.Salary <= previousSalary);
                }

                previousSalary = summary.Salary;
                previousLastName = summary.LastName;
            }

            Console.WriteLine("✅ Projection with ordering test passed!");
        }

        /// <summary>
        /// Tests projection with WHERE clause filtering.
        /// </summary>
        [Fact]
        public async Task TestProjectionWithFiltering()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing projection with filtering...");

            // Project with WHERE clause on original entity
            IEnumerable<DepartmentInfo> itDeptInfo = await _PersonRepository.Query()
                .Where(p => p.Department == "IT")
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary,
                    EmployeeName = p.FirstName + " " + p.LastName
                })
                .ExecuteAsync();

            List<DepartmentInfo> itList = itDeptInfo.ToList();

            Assert.True(itList.Count >= 1);
            Console.WriteLine($"Retrieved {itList.Count} IT department records");

            foreach (DepartmentInfo info in itList)
            {
                Console.WriteLine($"  {info.Department}: {info.EmployeeName} - ${info.Salary:N2}");
                Assert.Equal("IT", info.Department);
            }

            Console.WriteLine("✅ Projection with filtering test passed!");
        }

        /// <summary>
        /// Tests distinct operations on projected results.
        /// </summary>
        [Fact]
        public async Task TestDistinctProjections()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing distinct projections...");

            // Get distinct departments with their basic info
            IEnumerable<DepartmentSummary> distinctDepts = await _PersonRepository.Query()
                .Select(p => new DepartmentSummary
                {
                    DepartmentName = p.Department,
                    SampleSalaryRange = p.Salary > 60000 ? "High" : "Standard"
                })
                .Distinct()
                .ExecuteAsync();

            List<DepartmentSummary> deptList = distinctDepts.ToList();

            Console.WriteLine($"Retrieved {deptList.Count} distinct department summaries");

            foreach (DepartmentSummary dept in deptList)
            {
                Console.WriteLine($"  Department: {dept.DepartmentName} (Salary Range: {dept.SampleSalaryRange})");
                Assert.False(string.IsNullOrEmpty(dept.DepartmentName));
                Assert.Contains(dept.SampleSalaryRange, new[] { "High", "Standard" });
            }

            // Verify that we have unique combinations
            List<object> uniqueCombinations = deptList
                .Select(d => new { d.DepartmentName, d.SampleSalaryRange })
                .Distinct()
                .ToList<object>();

            Assert.Equal(deptList.Count, uniqueCombinations.Count);

            Console.WriteLine("✅ Distinct projections test passed!");
        }

        /// <summary>
        /// Tests projection with pagination (Take and Skip).
        /// </summary>
        [Fact]
        public async Task TestProjectionWithPagination()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing projection with pagination...");

            // First page: Take first 3 records
            IEnumerable<PersonSummary> page1 = await _PersonRepository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .OrderBy(s => s.LastName)
                .Take(3)
                .ExecuteAsync();

            List<PersonSummary> page1List = page1.ToList();

            Console.WriteLine($"Page 1: Retrieved {page1List.Count} records");
            foreach (PersonSummary summary in page1List)
            {
                Console.WriteLine($"  {summary.FirstName} {summary.LastName} - ${summary.Salary:N2}");
            }

            // Second page: Skip 3, take next 3
            IEnumerable<PersonSummary> page2 = await _PersonRepository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .OrderBy(s => s.LastName)
                .Skip(3)
                .Take(3)
                .ExecuteAsync();

            List<PersonSummary> page2List = page2.ToList();

            Console.WriteLine($"Page 2: Retrieved {page2List.Count} records");
            foreach (PersonSummary summary in page2List)
            {
                Console.WriteLine($"  {summary.FirstName} {summary.LastName} - ${summary.Salary:N2}");
            }

            // Verify no overlap between pages
            Assert.DoesNotContain(page1List, p1 => page2List.Any(p2 => p2.Email == p1.Email));

            // Verify ordering is maintained
            for (int i = 1; i < page1List.Count; i++)
            {
                Assert.True(string.Compare(page1List[i - 1].LastName, page1List[i].LastName, StringComparison.Ordinal) <= 0);
            }

            for (int i = 1; i < page2List.Count; i++)
            {
                Assert.True(string.Compare(page2List[i - 1].LastName, page2List[i].LastName, StringComparison.Ordinal) <= 0);
            }

            Console.WriteLine("✅ Projection with pagination test passed!");
        }

        /// <summary>
        /// Tests advanced projections with calculated fields and conditional logic.
        /// </summary>
        [Fact]
        public async Task TestAdvancedCalculatedProjections()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing advanced calculated projections...");

            // Project with complex calculations and conditional logic
            IEnumerable<AdvancedEmployeeMetrics> metrics = await _PersonRepository.Query()
                .Select(p => new AdvancedEmployeeMetrics
                {
                    EmployeeId = p.Id,
                    DisplayName = p.LastName + ", " + p.FirstName,
                    AgeGroup = p.Age < 25 ? "Junior" : p.Age < 35 ? "Mid-level" : p.Age < 50 ? "Senior" : "Executive",
                    SalaryTier = p.Salary < 50000 ? "Entry" : p.Salary < 70000 ? "Mid" : p.Salary < 90000 ? "Senior" : "Executive",
                    EstimatedAnnualBonus = p.Salary * 0.10m,
                    MonthlyTakeHome = p.Salary / 12 * 0.75m, // Rough estimate after taxes
                    DepartmentCode = p.Department.Substring(0, 2).ToUpper(),
                    IsEligibleForPromotion = p.Age >= 25 && p.Salary < 80000,
                    YearlyGrowthPotential = (90000 - p.Salary) / 5 // Spread over 5 years to reach $90K
                })
                .ExecuteAsync();

            List<AdvancedEmployeeMetrics> metricsList = metrics.ToList();

            Assert.True(metricsList.Count >= 8);
            Console.WriteLine($"Retrieved {metricsList.Count} advanced employee metrics");

            foreach (AdvancedEmployeeMetrics metric in metricsList)
            {
                Console.WriteLine($"  {metric.DisplayName} (ID: {metric.EmployeeId})");
                Console.WriteLine($"    Age Group: {metric.AgeGroup}, Salary Tier: {metric.SalaryTier}");
                Console.WriteLine($"    Dept Code: {metric.DepartmentCode}");
                Console.WriteLine($"    Est. Bonus: ${metric.EstimatedAnnualBonus:N2}");
                Console.WriteLine($"    Monthly Take-Home: ${metric.MonthlyTakeHome:N2}");
                Console.WriteLine($"    Promotion Eligible: {metric.IsEligibleForPromotion}");
                Console.WriteLine($"    Yearly Growth Potential: ${metric.YearlyGrowthPotential:N2}");

                // Verify calculated fields
                Assert.True(metric.EmployeeId > 0);
                Assert.Contains(",", metric.DisplayName);
                Assert.Contains(metric.AgeGroup, new[] { "Junior", "Mid-level", "Senior", "Executive" });
                Assert.Contains(metric.SalaryTier, new[] { "Entry", "Mid", "Senior", "Executive" });
                Assert.True(metric.EstimatedAnnualBonus > 0);
                Assert.True(metric.MonthlyTakeHome > 0);
                Assert.Equal(2, metric.DepartmentCode.Length);
            }

            Console.WriteLine("✅ Advanced calculated projections test passed!");
        }

        /// <summary>
        /// Tests SQL generation and capture for projection operations.
        /// </summary>
        [Fact]
        public async Task TestProjectionSqlGeneration()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing projection SQL generation...");

            _PersonRepository.CaptureSql = true;

            // Execute projection with SQL capture
            IEnumerable<PersonSummary> summaries = await _PersonRepository.Query()
                .Where(p => p.Age > 30)
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .OrderBy(s => s.LastName)
                .Take(5)
                .ExecuteAsync();

            List<PersonSummary> summaryList = summaries.ToList(); // Force execution

            Assert.NotNull(_PersonRepository.LastExecutedSql);
            string sql = _PersonRepository.LastExecutedSql.ToUpper();

            Assert.Contains("SELECT", sql);
            Assert.Contains("WHERE", sql);
            Assert.Contains("ORDER BY", sql);
            Assert.Contains("LIMIT", sql);

            Console.WriteLine($"Generated SQL: {_PersonRepository.LastExecutedSql}");
            Console.WriteLine($"Retrieved {summaryList.Count} projected records");

            // Verify the projection worked correctly
            foreach (PersonSummary summary in summaryList)
            {
                Assert.False(string.IsNullOrEmpty(summary.FirstName));
                Assert.False(string.IsNullOrEmpty(summary.LastName));
                Assert.False(string.IsNullOrEmpty(summary.Email));
                Assert.True(summary.Salary > 0);
            }

            Console.WriteLine("✅ Projection SQL generation test passed!");
        }

        /// <summary>
        /// Tests async projection operations with various result types.
        /// </summary>
        [Fact]
        public async Task TestAsyncProjectionOperations()
        {
            if (_SkipTests) return;

            Console.WriteLine("Testing async projection operations...");

            // Test async projection with multiple result types
            Task<List<PersonSummary>> summariesTask = _PersonRepository.Query()
                .Select(p => new PersonSummary
                {
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    Email = p.Email,
                    Salary = p.Salary
                })
                .ExecuteAsync()
                .ContinueWith(t => t.Result.ToList());

            Task<List<DepartmentInfo>> deptInfoTask = _PersonRepository.Query()
                .Select(p => new DepartmentInfo
                {
                    Department = p.Department,
                    Salary = p.Salary,
                    EmployeeName = p.FirstName + " " + p.LastName
                })
                .ExecuteAsync()
                .ContinueWith(t => t.Result.ToList());

            // Wait for both tasks to complete
            List<PersonSummary> summaries = await summariesTask;
            List<DepartmentInfo> deptInfo = await deptInfoTask;

            Assert.True(summaries.Count >= 8);
            Assert.True(deptInfo.Count >= 8);

            Console.WriteLine($"Async retrieved {summaries.Count} summaries and {deptInfo.Count} dept info records");

            // Verify async results
            Assert.All(summaries, s => Assert.False(string.IsNullOrEmpty(s.FirstName)));
            Assert.All(deptInfo, d => Assert.False(string.IsNullOrEmpty(d.Department)));

            Console.WriteLine("✅ Async projection operations test passed!");
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
                using MySqlConnection connection = new MySqlConnection(TestConnectionString);
                connection.Open();

                using MySqlCommand command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();

                Console.WriteLine("✅ MySQL projection tests enabled - database connection successful");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  MySQL projection tests disabled - could not connect to database: {ex.Message}");
                Console.WriteLine("To enable MySQL projection tests:");
                Console.WriteLine("1. Start MySQL server on localhost:3306");
                Console.WriteLine($"2. Create database: CREATE DATABASE {TestDatabaseName};");
                Console.WriteLine("3. Create user: CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'test_password';");
                Console.WriteLine($"4. Grant permissions: GRANT ALL PRIVILEGES ON {TestDatabaseName}.* TO 'test_user'@'localhost';");
                _SkipTests = true;
            }
        }

        /// <summary>
        /// Creates the people table for projection testing.
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

            // Create table
            _PersonRepository.ExecuteSql(@"
                CREATE TABLE people (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(128) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(32) NOT NULL,
                    INDEX idx_department (department),
                    INDEX idx_age (age),
                    INDEX idx_salary (salary),
                    INDEX idx_last_name (last)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

            Console.WriteLine("✅ People table created for projection testing");
        }

        /// <summary>
        /// Inserts diverse test data for comprehensive projection testing.
        /// </summary>
        private void InsertTestData()
        {
            if (_SkipTests) return;

            Console.WriteLine("Seeding test data for projection functionality...");

            List<Person> testPeople = new List<Person>
            {
                // IT Department
                new Person { FirstName = "Alice", LastName = "Anderson", Age = 28, Email = "alice.anderson@company.com", Salary = 72000, Department = "IT" },
                new Person { FirstName = "Bob", LastName = "Baker", Age = 32, Email = "bob.baker@company.com", Salary = 78000, Department = "IT" },
                new Person { FirstName = "Carol", LastName = "Chen", Age = 29, Email = "carol.chen@company.com", Salary = 75000, Department = "IT" },

                // HR Department
                new Person { FirstName = "David", LastName = "Davis", Age = 45, Email = "david.davis@company.com", Salary = 68000, Department = "HR" },
                new Person { FirstName = "Emma", LastName = "Evans", Age = 38, Email = "emma.evans@company.com", Salary = 65000, Department = "HR" },

                // Sales Department
                new Person { FirstName = "Frank", LastName = "Foster", Age = 26, Email = "frank.foster@company.com", Salary = 55000, Department = "Sales" },
                new Person { FirstName = "Grace", LastName = "Garcia", Age = 31, Email = "grace.garcia@company.com", Salary = 58000, Department = "Sales" },
                new Person { FirstName = "Henry", LastName = "Harris", Age = 35, Email = "henry.harris@company.com", Salary = 62000, Department = "Sales" },

                // Finance Department
                new Person { FirstName = "Isabel", LastName = "Ivanov", Age = 41, Email = "isabel.ivanov@company.com", Salary = 85000, Department = "Finance" },
                new Person { FirstName = "Jack", LastName = "Johnson", Age = 33, Email = "jack.johnson@company.com", Salary = 80000, Department = "Finance" },

                // Marketing Department
                new Person { FirstName = "Kate", LastName = "Kelly", Age = 27, Email = "kate.kelly@company.com", Salary = 52000, Department = "Marketing" },
                new Person { FirstName = "Luke", LastName = "Lopez", Age = 30, Email = "luke.lopez@company.com", Salary = 54000, Department = "Marketing" }
            };

            foreach (Person person in testPeople)
            {
                _PersonRepository.Create(person);
            }

            Console.WriteLine("✅ Test data seeded successfully");
            Console.WriteLine($"   Total People: {testPeople.Count}");
            Console.WriteLine($"   Departments: IT (3), HR (2), Sales (3), Finance (2), Marketing (2)");
            Console.WriteLine($"   Age Range: 26-45, Salary Range: $52K-$85K");
        }

        #endregion

        #region Projection-Classes

        /// <summary>
        /// Summary information for a person containing key details.
        /// </summary>
        public class PersonSummary
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Email { get; set; }
            public decimal Salary { get; set; }
        }

        /// <summary>
        /// Department information with employee details.
        /// </summary>
        public class DepartmentInfo
        {
            public string Department { get; set; }
            public decimal Salary { get; set; }
            public string EmployeeName { get; set; }
        }

        /// <summary>
        /// Department summary for distinct operations.
        /// </summary>
        public class DepartmentSummary
        {
            public string DepartmentName { get; set; }
            public string SampleSalaryRange { get; set; }
        }

        /// <summary>
        /// Comprehensive employee profile with computed fields.
        /// </summary>
        public class EmployeeProfile
        {
            public string FullName { get; set; }
            public string ContactInfo { get; set; }
            public string WorkInfo { get; set; }
            public decimal SalaryInfo { get; set; }
            public string AgeCategory { get; set; }
            public bool IsHighEarner { get; set; }
            public int YearsToRetirement { get; set; }
        }

        /// <summary>
        /// Advanced employee metrics with complex calculations.
        /// </summary>
        public class AdvancedEmployeeMetrics
        {
            public int EmployeeId { get; set; }
            public string DisplayName { get; set; }
            public string AgeGroup { get; set; }
            public string SalaryTier { get; set; }
            public decimal EstimatedAnnualBonus { get; set; }
            public decimal MonthlyTakeHome { get; set; }
            public string DepartmentCode { get; set; }
            public bool IsEligibleForPromotion { get; set; }
            public decimal YearlyGrowthPotential { get; set; }
        }

        #endregion
    }
}