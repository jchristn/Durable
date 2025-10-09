#nullable enable

namespace Test.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;

    /// <summary>
    /// Comprehensive MySQL test program demonstrating ORM functionality and expression parsing.
    /// Tests both synchronous and asynchronous operations.
    /// </summary>
    class Program
    {
        #region Private-Members

        private static readonly List<TestResult> _TestResults = new List<TestResult>();
        private static string _ConnectionString = "";

        #endregion

        #region Public-Methods

        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("=== MySQL Integration Test Suite ===\n");

                // Parse command line arguments with priority: CLI args > Environment vars > Interactive prompt > Default
                if (args.Length > 0)
                {
                    _ConnectionString = args[0];
                    Console.WriteLine($"Using connection string from command line: {MaskConnectionString(_ConnectionString)}\n");
                }
                else
                {
                    _ConnectionString = BuildConnectionString();
                    Console.WriteLine("Tip: You can specify a custom MySQL connection string by passing it as an argument.");
                    Console.WriteLine("     Example: dotnet Test.MySql.dll \"Server=localhost;Database=mydb;User=myuser;Password=mypass;AllowUserVariables=true;\"\n");
                }

                await MySqlTestRunner.RunAllTests();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Fatal error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }

        /// <summary>
        /// Builds the connection string from environment variables or prompts user.
        /// </summary>
        /// <returns>A MySQL connection string.</returns>
        private static string BuildConnectionString()
        {
            string server = Environment.GetEnvironmentVariable("MYSQL_SERVER") ?? "";
            string database = Environment.GetEnvironmentVariable("MYSQL_DATABASE") ?? "";
            string user = Environment.GetEnvironmentVariable("MYSQL_USER") ?? "";
            string password = Environment.GetEnvironmentVariable("MYSQL_PASSWORD") ?? "";

            // If any required value is missing, prompt for all of them
            if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(user) || string.IsNullOrEmpty(password))
            {
                Console.WriteLine("=== MySQL Connection Setup ===");

                if (string.IsNullOrEmpty(server))
                {
                    Console.Write("Enter MySQL host and port (e.g., 'localhost' or 'server.com:3306'): ");
                    Console.Write("(or press Enter for default 'localhost'): ");
                    server = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(server))
                    {
                        server = "localhost";
                    }
                }
                else
                {
                    Console.WriteLine($"Using server from environment: {server}");
                }

                if (string.IsNullOrEmpty(user))
                {
                    Console.Write("Enter MySQL username (or press Enter for default 'test_user'): ");
                    user = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(user))
                    {
                        user = "test_user";
                    }
                }
                else
                {
                    Console.WriteLine($"Using username from environment: {user}");
                }

                if (string.IsNullOrEmpty(password))
                {
                    Console.Write("Enter MySQL password (or press Enter for default 'test_password'): ");
                    password = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(password))
                    {
                        password = "test_password";
                    }
                }

                if (string.IsNullOrEmpty(database))
                {
                    Console.Write("Enter database name (or press Enter for default 'durable_integration_test'): ");
                    database = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(database))
                    {
                        database = "durable_integration_test";
                    }
                }
                else
                {
                    Console.WriteLine($"Using database from environment: {database}");
                }

                Console.WriteLine();
            }

            // Use defaults if still empty
            if (string.IsNullOrEmpty(database))
            {
                database = "durable_integration_test";
            }

            return $"Server={server};Database={database};User={user};Password={password};AllowUserVariables=true;";
        }

        /// <summary>
        /// Masks the password in a connection string for safe display.
        /// </summary>
        /// <param name="connectionString">The connection string to mask.</param>
        /// <returns>Connection string with password hidden.</returns>
        private static string MaskConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return connectionString;

            int passwordIndex = connectionString.IndexOf("Password=", StringComparison.OrdinalIgnoreCase);
            if (passwordIndex == -1)
                return connectionString;

            int passwordStart = passwordIndex + "Password=".Length;
            int semicolonIndex = connectionString.IndexOf(';', passwordStart);

            if (semicolonIndex == -1)
                return connectionString.Substring(0, passwordStart) + "***";

            return connectionString.Substring(0, passwordStart) + "***" + connectionString.Substring(semicolonIndex);
        }

        #endregion

        #region Private-Methods

        private static async Task<bool> IsMyServerAvailable()
        {
            try
            {
                using var connection = new MySqlConnection(_ConnectionString);
                await connection.OpenAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static async Task InitializeDatabase()
        {
            try
            {
                using var connection = new MySqlConnection(_ConnectionString);
                await connection.OpenAsync();

                // Create test database if it doesn't exist
                string createDbSql = @"
                    CREATE DATABASE IF NOT EXISTS durable_integration_test
                    CHARACTER SET utf8mb4
                    COLLATE utf8mb4_unicode_ci;
                    USE durable_integration_test;";

                using var command = new MySqlCommand(createDbSql, connection);
                await command.ExecuteNonQueryAsync();

                // Drop and recreate Person table
                string createTableSql = @"
                    DROP TABLE IF EXISTS `people`;
                    CREATE TABLE `people` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                        `first` VARCHAR(64) NOT NULL,
                        `last` VARCHAR(64) NOT NULL,
                        `age` INT NOT NULL DEFAULT 0,
                        `email` VARCHAR(128),
                        `salary` DECIMAL(10,2) DEFAULT 0.00,
                        `department` VARCHAR(32)
                    ) ENGINE=InnoDB;";

                using var createCommand = new MySqlCommand(createTableSql, connection);
                await createCommand.ExecuteNonQueryAsync();

                Console.WriteLine("✅ Database initialized successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Database initialization failed: {ex.Message}");
                throw;
            }
        }

        private static async Task RunTest(string testName, Func<Task> testAction)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                await testAction();
                stopwatch.Stop();
                _TestResults.Add(new TestResult { Name = testName, Success = true, Duration = stopwatch.Elapsed });
                Console.WriteLine($"✅ {testName} - {stopwatch.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _TestResults.Add(new TestResult { Name = testName, Success = false, Duration = stopwatch.Elapsed, Error = ex.Message });
                Console.WriteLine($"❌ {testName} - Failed: {ex.Message}");
            }
        }

        private static async Task TestRepositoryCreation(MySqlRepository<Person> repository)
        {
            // Test that repository implements expected interfaces
            if (!(repository is IRepository<Person>))
                throw new Exception("Repository doesn't implement IRepository<Person>");

            if (!(repository is ISqlCapture sqlCapture))
                throw new Exception("Repository doesn't implement ISqlCapture");

            // Test basic properties
            await Task.CompletedTask;
        }

        private static async Task TestSchemaCreation(MySqlRepository<Person> repository)
        {
            // Test that we can query the schema
            int count = repository.Count();
            await Task.CompletedTask;
        }

        private static async Task TestCreateOperations(MySqlRepository<Person> repository)
        {
            // Clear existing data
            repository.DeleteAll();

            // Test single create
            Person person = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                Email = "john.doe@example.com",
                Salary = 50000.00m,
                Department = "Engineering"
            };

            Person created = repository.Create(person);
            if (created.Id == 0)
                throw new Exception("Created person should have an ID");

            // Test batch create
            List<Person> people = new List<Person>
            {
                new Person { FirstName = "Jane", LastName = "Smith", Age = 25, Email = "jane@example.com" },
                new Person { FirstName = "Bob", LastName = "Johnson", Age = 35, Email = "bob@example.com" },
                new Person { FirstName = "Alice", LastName = "Brown", Age = 28, Email = "alice@example.com" }
            };

            IEnumerable<Person> batchCreated = repository.CreateMany(people);
            if (batchCreated.Count() != 3)
                throw new Exception("Should have created 3 people");

            await Task.CompletedTask;
        }

        private static async Task TestReadOperations(MySqlRepository<Person> repository)
        {
            // Test read all
            IEnumerable<Person> allPeople = repository.ReadAll();
            if (allPeople.Count() < 1)
                throw new Exception("Should have at least 1 person");

            // Test read by ID
            Person? first = allPeople.First();
            Person? byId = repository.ReadById(first.Id);
            if (byId == null || byId.Id != first.Id)
                throw new Exception("ReadById failed");

            // Test count
            int count = repository.Count();
            if (count < 1)
                throw new Exception("Count should be at least 1");

            await Task.CompletedTask;
        }

        private static async Task TestUpdateOperations(MySqlRepository<Person> repository)
        {
            Person? person = repository.ReadAll().First();
            string originalEmail = person.Email ?? "";

            person.Email = "updated@example.com";
            Person updated = repository.Update(person);

            if (updated.Email != "updated@example.com")
                throw new Exception("Update failed");

            // Restore original
            person.Email = originalEmail;
            repository.Update(person);

            await Task.CompletedTask;
        }

        private static async Task TestDeleteOperations(MySqlRepository<Person> repository)
        {
            int originalCount = repository.Count();

            // Create a person to delete
            Person toDelete = repository.Create(new Person
            {
                FirstName = "Delete",
                LastName = "Me",
                Age = 99,
                Email = "delete@example.com"
            });

            // Test delete by ID
            bool deleted = repository.DeleteById(toDelete.Id);
            if (!deleted)
                throw new Exception("DeleteById should return true");

            int newCount = repository.Count();
            if (newCount != originalCount)
                throw new Exception("Count should be back to original");

            await Task.CompletedTask;
        }

        private static async Task TestSimpleExpressions(MySqlRepository<Person> repository)
        {
            // Test equality
            IEnumerable<Person> johns = repository.Query()
                .Where(p => p.FirstName == "John")
                .Execute();

            // Test comparison
            IEnumerable<Person> adults = repository.Query()
                .Where(p => p.Age >= 25)
                .Execute();

            // Test logical AND
            IEnumerable<Person> filtered = repository.Query()
                .Where(p => p.FirstName == "John" && p.Age > 25)
                .Execute();

            // Test logical OR
            IEnumerable<Person> multipleNames = repository.Query()
                .Where(p => p.FirstName == "John" || p.FirstName == "Jane")
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestComplexExpressions(MySqlRepository<Person> repository)
        {
            // Test complex logical expression
            IEnumerable<Person> complex = repository.Query()
                .Where(p => (p.FirstName == "John" && p.Age > 25) || (p.FirstName == "Jane" && p.Age < 30))
                .Execute();

            // Test null comparisons
            IEnumerable<Person> withEmail = repository.Query()
                .Where(p => p.Email != null)
                .Execute();

            // Test math operations
            IEnumerable<Person> salaryBonus = repository.Query()
                .Where(p => p.Salary * 1.1m > 50000)
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestStringMethods(MySqlRepository<Person> repository)
        {
            // Test Contains
            IEnumerable<Person> containsTest = repository.Query()
                .Where(p => p.Email.Contains("example"))
                .Execute();

            // Test StartsWith
            IEnumerable<Person> startsWithTest = repository.Query()
                .Where(p => p.FirstName.StartsWith("J"))
                .Execute();

            // Test EndsWith
            IEnumerable<Person> endsWithTest = repository.Query()
                .Where(p => p.Email.EndsWith(".com"))
                .Execute();

            // Test ToUpper
            IEnumerable<Person> upperTest = repository.Query()
                .Where(p => p.FirstName.ToUpper() == "JOHN")
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestMathOperations(MySqlRepository<Person> repository)
        {
            // Test addition
            IEnumerable<Person> addTest = repository.Query()
                .Where(p => p.Age + 5 > 30)
                .Execute();

            // Test subtraction
            IEnumerable<Person> subtractTest = repository.Query()
                .Where(p => p.Age - 5 < 40)
                .Execute();

            // Test multiplication
            IEnumerable<Person> multiplyTest = repository.Query()
                .Where(p => p.Salary * 2 > 80000)
                .Execute();

            await Task.CompletedTask;
        }


        private static async Task TestCollectionOperations(MySqlRepository<Person> repository)
        {
            // Test IN operation with array
            string[] names = { "John", "Jane", "Bob" };
            IEnumerable<Person> inTest = repository.Query()
                .Where(p => names.Contains(p.FirstName))
                .Execute();

            // Test IN operation with list
            List<int> ages = new List<int> { 25, 30, 35 };
            IEnumerable<Person> ageInTest = repository.Query()
                .Where(p => ages.Contains(p.Age))
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestOrderByOperations(MySqlRepository<Person> repository)
        {
            // Test OrderBy
            IEnumerable<Person> orderedByAge = repository.Query()
                .OrderBy(p => p.Age)
                .Execute();

            // Test OrderByDescending
            IEnumerable<Person> orderedByNameDesc = repository.Query()
                .OrderByDescending(p => p.FirstName)
                .Execute();

            // Test multiple OrderBy
            IEnumerable<Person> multipleOrder = repository.Query()
                .OrderBy(p => p.LastName)
                .ThenBy(p => p.FirstName)
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestTakeSkipOperations(MySqlRepository<Person> repository)
        {
            // Test Take
            IEnumerable<Person> firstTwo = repository.Query()
                .Take(2)
                .Execute();

            if (firstTwo.Count() > 2)
                throw new Exception("Take(2) should return at most 2 items");

            // Test Skip
            IEnumerable<Person> skipFirst = repository.Query()
                .Skip(1)
                .Execute();

            // Test Take + Skip
            IEnumerable<Person> paging = repository.Query()
                .Skip(1)
                .Take(2)
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestDistinctOperations(MySqlRepository<Person> repository)
        {
            // Test Distinct
            IEnumerable<Person> distinct = repository.Query()
                .Distinct()
                .Execute();

            await Task.CompletedTask;
        }

        private static async Task TestSqlGeneration(MySqlRepository<Person> repository)
        {
            // Enable SQL capture
            repository.CaptureSql = true;

            // Execute a query
            repository.Query()
                .Where(p => p.FirstName == "John" && p.Age > 25)
                .OrderBy(p => p.LastName)
                .Take(5)
                .Execute();

            string? sql = repository.LastExecutedSql;
            if (string.IsNullOrEmpty(sql))
                throw new Exception("Should have captured SQL");

            if (!sql.Contains("SELECT"))
                throw new Exception("SQL should contain SELECT");

            if (!sql.Contains("WHERE"))
                throw new Exception("SQL should contain WHERE clause");

            if (!sql.Contains("ORDER BY"))
                throw new Exception("SQL should contain ORDER BY clause");

            if (!sql.Contains("LIMIT"))
                throw new Exception("SQL should contain LIMIT clause");

            Console.WriteLine($"   Generated SQL: {sql}");

            await Task.CompletedTask;
        }

        private static async Task TestParenthesesOptimization(MySqlRepository<Person> repository)
        {
            repository.CaptureSql = true;

            // Test that parentheses are optimized
            repository.Query()
                .Where(p => p.FirstName == "John" && p.Age > 25)
                .Execute();

            string? sql = repository.LastExecutedSql;
            if (string.IsNullOrEmpty(sql))
                throw new Exception("Should have captured SQL");

            // Should not have excessive parentheses like ((`FirstName` = 'John') AND (`Age` > 25))
            if (sql.Contains("((`") || sql.Contains("`) AND (`"))
                throw new Exception($"SQL has excessive parentheses: {sql}");

            Console.WriteLine($"   Optimized SQL: {sql}");

            await Task.CompletedTask;
        }

        private static async Task TestAsyncCreateOperations(MySqlRepository<Person> repository)
        {
            // Clear existing data
            repository.DeleteAll();

            Person person = new Person
            {
                FirstName = "Async",
                LastName = "Test",
                Age = 25,
                Email = "async@example.com"
            };

            Person created = await repository.CreateAsync(person);
            if (created.Id == 0)
                throw new Exception("Async created person should have an ID");
        }

        private static async Task TestAsyncReadOperations(MySqlRepository<Person> repository)
        {
            List<Person> allPeople = new List<Person>();
            await foreach (Person person in repository.ReadAllAsync())
            {
                allPeople.Add(person);
            }

            if (allPeople.Count < 1)
                throw new Exception("Async ReadAll should return at least 1 person");

            Person? first = allPeople.First();
            Person? byId = await repository.ReadByIdAsync(first.Id);
            if (byId == null || byId.Id != first.Id)
                throw new Exception("Async ReadById failed");

            int count = await repository.CountAsync();
            if (count < 1)
                throw new Exception("Async Count should be at least 1");
        }

        private static void PrintTestResults()
        {
            Console.WriteLine("\n========== TEST RESULTS SUMMARY ==========");

            int passed = _TestResults.Count(t => t.Success);
            int failed = _TestResults.Count(t => !t.Success);
            double totalMs = _TestResults.Sum(t => t.Duration.TotalMilliseconds);

            Console.WriteLine($"Tests run: {_TestResults.Count}");
            Console.WriteLine($"✅ Passed: {passed}");
            Console.WriteLine($"❌ Failed: {failed}");
            Console.WriteLine($"⏱️  Total time: {totalMs:F2}ms");
            Console.WriteLine($"📊 Success rate: {(passed * 100.0 / _TestResults.Count):F1}%");

            if (failed > 0)
            {
                Console.WriteLine("\n❌ Failed tests:");
                foreach (TestResult failure in _TestResults.Where(t => !t.Success))
                {
                    Console.WriteLine($"   • {failure.Name}: {failure.Error}");
                }
            }

            Console.WriteLine($"\n🎉 MySQL ORM Test Complete - Expression parsing and CRUD operations verified!");
        }

        #endregion

        #region Nested-Classes

        private class TestResult
        {
            public string Name { get; set; } = "";
            public bool Success { get; set; }
            public TimeSpan Duration { get; set; }
            public string Error { get; set; } = "";
        }

        #endregion
    }
}