using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Durable;
using Durable.SqlServer;
using Test.Shared;
using Microsoft.Data.SqlClient;

namespace Test.SqlServer
{
    /// <summary>
    /// Comprehensive integration tests for SQL Server implementation.
    /// These tests require a running SQL Server and will be skipped if connection fails.
    ///
    /// To run these tests:
    /// 1. Ensure SQL Server is running on localhost:1433
    /// 2. Create database: CREATE DATABASE durable_integration_test;
    /// 3. Create user: CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'test_password';
    /// 4. Grant permissions: GRANT ALL PRIVILEGES ON durable_integration_test.* TO 'test_user'@'localhost';
    /// </summary>
    public class SqlServerIntegrationTests : IDisposable
    {
        #region Private-Members

        private const string TestConnectionString = "Server=view.homedns.org,1433;Database=durable_integration_test;User=sa;Password=P@ssw0rd4Sql;TrustServerCertificate=true;Encrypt=false;";
        private const string TestDatabaseName = "durable_integration_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the test class by setting up the test database.
        /// </summary>
        public SqlServerIntegrationTests()
        {
            lock (_TestLock)
            {
                if (!_DatabaseSetupComplete)
                {
                    SetupTestDatabase();
                    _DatabaseSetupComplete = true;
                }
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests basic repository instantiation and interface compliance.
        /// </summary>
        [Fact]
        public void CanCreateRepository()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);

            Assert.NotNull(repository);
            Assert.IsAssignableFrom<IRepository<Person>>(repository);
            Assert.IsAssignableFrom<IBatchInsertConfiguration>(repository);
            Assert.IsAssignableFrom<ISqlCapture>(repository);
        }

        /// <summary>
        /// Tests database connectivity and basic SQL execution.
        /// </summary>
        [Fact]
        public async Task CanConnectToDatabase()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Test basic connectivity with a simple SQL operation that affects rows
            // Create a test record and then delete it to verify SQL execution
            int insertResult = await repository.ExecuteSqlAsync(
                "INSERT INTO people (first, last, age, email, salary, department) VALUES ('Test', 'User', 25, 'test@example.com', 50000, 'Test')"
            );
            Assert.Equal(1, insertResult); // Should affect 1 row

            // Clean up - delete the test record
            int deleteResult = await repository.ExecuteSqlAsync("DELETE FROM people WHERE email = 'test@example.com'");
            Assert.Equal(1, deleteResult); // Should affect 1 row
        }

        /// <summary>
        /// Tests complete CRUD operations lifecycle.
        /// </summary>
        [Fact]
        public async Task CanPerformCrudOperations()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // CREATE
            Person newPerson = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                Email = "john.doe@test.com",
                Salary = 75000,
                Department = "Engineering"
            };

            Person created = await repository.CreateAsync(newPerson);
            Assert.NotNull(created);
            Assert.True(created.Id > 0);
            Assert.Equal("John", created.FirstName);

            // READ
            Person retrieved = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(retrieved);
            Assert.Equal(created.Id, retrieved.Id);
            Assert.Equal("John", retrieved.FirstName);

            // UPDATE
            retrieved.Age = 31;
            Person updated = await repository.UpdateAsync(retrieved);
            Assert.Equal(31, updated.Age);

            // Verify update
            Person verified = await repository.ReadByIdAsync(created.Id);
            Assert.Equal(31, verified.Age);

            // DELETE
            bool deleted = await repository.DeleteAsync(updated);
            Assert.True(deleted);

            // Verify deletion
            Person shouldBeNull = await repository.ReadByIdAsync(created.Id);
            Assert.Null(shouldBeNull);
        }

        /// <summary>
        /// Tests batch insert operations with different configurations.
        /// </summary>
        [Fact]
        public async Task CanPerformBatchInsert()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Generate test data
            List<Person> people = new List<Person>();
            for (int i = 0; i < 100; i++)
            {
                people.Add(new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = 20 + (i % 50),
                    Email = $"person{i}@test.com",
                    Salary = 50000 + (i * 1000),
                    Department = i % 2 == 0 ? "Engineering" : "Marketing"
                });
            }

            // Batch insert
            IEnumerable<Person> inserted = await repository.CreateManyAsync(people);
            List<Person> insertedList = inserted.ToList();

            Assert.Equal(100, insertedList.Count);
            Assert.All(insertedList, p => Assert.True(p.Id > 0));

            // Verify count in database
            int count = await repository.CountAsync();
            Assert.True(count >= 100);

            // Clean up
            await repository.DeleteAllAsync();
        }

        /// <summary>
        /// Tests LINQ query operations and expressions.
        /// </summary>
        [Fact]
        public async Task CanPerformLinqQueries()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Create test data
            Person[] testPeople = new[]
            {
                new Person { FirstName = "Alice", LastName = "Johnson", Age = 25, Email = "alice@test.com", Salary = 60000, Department = "Engineering" },
                new Person { FirstName = "Bob", LastName = "Smith", Age = 35, Email = "bob@test.com", Salary = 80000, Department = "Engineering" },
                new Person { FirstName = "Carol", LastName = "Williams", Age = 28, Email = "carol@test.com", Salary = 70000, Department = "Marketing" },
                new Person { FirstName = "David", LastName = "Brown", Age = 42, Email = "david@test.com", Salary = 90000, Department = "Sales" }
            };

            await repository.CreateManyAsync(testPeople);

            // Test Where clause
            List<Person> engineeringPeople = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == "Engineering"))
            {
                engineeringPeople.Add(person);
            }
            Assert.Equal(2, engineeringPeople.Count);

            // Test complex Where with AND
            List<Person> youngEngineers = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == "Engineering" && p.Age < 30))
            {
                youngEngineers.Add(person);
            }
            Assert.Single(youngEngineers);
            Assert.Equal("Alice", youngEngineers.First().FirstName);

            // Test OrderBy with Take
            List<Person> highestPaid = repository.Query()
                .OrderByDescending(p => p.Salary)
                .Take(2)
                .Execute()
                .ToList();
            Assert.Equal(2, highestPaid.Count);
            Assert.Equal("David", highestPaid.First().FirstName);

            // Test aggregates
            decimal avgSalary = await repository.AverageAsync(p => p.Salary);
            Assert.True(avgSalary > 0);

            int engineerCount = await repository.CountAsync(p => p.Department == "Engineering");
            Assert.Equal(2, engineerCount);

            // Clean up
            await repository.DeleteAllAsync();
        }

        /// <summary>
        /// Tests transaction operations including commit and rollback.
        /// </summary>
        [Fact]
        public async Task CanPerformTransactions()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Test successful transaction
            using (ITransaction transaction = await repository.BeginTransactionAsync())
            {
                Person person1 = new Person
                {
                    FirstName = "Trans1",
                    LastName = "Test",
                    Age = 30,
                    Email = "trans1@test.com",
                    Salary = 50000,
                    Department = "IT"
                };

                Person person2 = new Person
                {
                    FirstName = "Trans2",
                    LastName = "Test",
                    Age = 25,
                    Email = "trans2@test.com",
                    Salary = 55000,
                    Department = "IT"
                };

                await repository.CreateAsync(person1, transaction);
                await repository.CreateAsync(person2, transaction);

                await transaction.CommitAsync();
            }

            // Verify both records were inserted
            int count = await repository.CountAsync(p => p.Department == "IT");
            Assert.Equal(2, count);

            // Test rollback transaction
            int initialCount = await repository.CountAsync();

            try
            {
                ITransaction transaction = await repository.BeginTransactionAsync();

                Person person3 = new Person
                {
                    FirstName = "Trans3",
                    LastName = "Test",
                    Age = 35,
                    Email = "trans3@test.com",
                    Salary = 60000,
                    Department = "IT"
                };

                await repository.CreateAsync(person3, transaction);

                // Force rollback by not committing
                await transaction.RollbackAsync();
            }
            catch
            {
                // Expected if transaction is already rolled back
            }

            // Verify rollback worked
            int finalCount = await repository.CountAsync();
            Assert.Equal(initialCount, finalCount);

            // Clean up
            await repository.DeleteAllAsync();
        }

        /// <summary>
        /// Tests batch update and delete operations.
        /// </summary>
        [Fact]
        public async Task CanPerformBatchOperations()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Create test data
            Person[] testPeople = new[]
            {
                new Person { FirstName = "Batch1", LastName = "Test", Age = 25, Email = "batch1@test.com", Salary = 50000, Department = "IT" },
                new Person { FirstName = "Batch2", LastName = "Test", Age = 30, Email = "batch2@test.com", Salary = 60000, Department = "IT" },
                new Person { FirstName = "Batch3", LastName = "Test", Age = 35, Email = "batch3@test.com", Salary = 70000, Department = "HR" }
            };

            await repository.CreateManyAsync(testPeople);

            // Test batch update field
            int updatedCount = await repository.UpdateFieldAsync(
                p => p.Department == "IT",
                p => p.Salary,
                75000
            );
            Assert.Equal(2, updatedCount);

            // Verify batch update
            List<Person> itPeople = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == "IT"))
            {
                itPeople.Add(person);
            }
            Assert.All(itPeople, p => Assert.Equal(75000, p.Salary));

            // Test batch delete
            int deletedCount = await repository.BatchDeleteAsync(p => p.Department == "IT");
            Assert.Equal(2, deletedCount);

            // Verify only HR person remains
            int remainingCount = await repository.CountAsync();
            Assert.Equal(1, remainingCount);

            Person remaining = await repository.ReadFirstAsync();
            Assert.Equal("HR", remaining.Department);

            // Clean up
            await repository.DeleteAllAsync();
        }

        /// <summary>
        /// Tests query builder advanced features.
        /// </summary>
        [Fact]
        public async Task CanUseAdvancedQueryBuilder()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Create test data
            Person[] testPeople = new[]
            {
                new Person { FirstName = "Query1", LastName = "Test", Age = 25, Email = "query1@test.com", Salary = 50000, Department = "Engineering" },
                new Person { FirstName = "Query2", LastName = "Test", Age = 30, Email = "query2@test.com", Salary = 60000, Department = "Engineering" },
                new Person { FirstName = "Query3", LastName = "Test", Age = 35, Email = "query3@test.com", Salary = 70000, Department = "Marketing" },
                new Person { FirstName = "Query4", LastName = "Test", Age = 40, Email = "query4@test.com", Salary = 80000, Department = "Marketing" }
            };

            await repository.CreateManyAsync(testPeople);

            // Test complex query with multiple conditions
            IQueryBuilder<Person> query = repository.Query()
                .Where(p => p.Department == "Engineering")
                .Where(p => p.Age >= 30)
                .OrderBy(p => p.Salary)
                .Take(1);

            IEnumerable<Person> result = await query.ExecuteAsync();
            List<Person> resultList = result.ToList();

            Assert.Single(resultList);
            Assert.Equal("Query2", resultList.First().FirstName);

            // Test query with SQL capture
            repository.CaptureSql = true;

            // First test: Try a simple repository method to verify SQL capture works
            int count = await repository.CountAsync();  // Simple count without predicate

            Assert.NotNull(repository.LastExecutedSql);
            Assert.Contains("COUNT", repository.LastExecutedSql);

            // Second test: Try query builder
            IQueryBuilder<Person> captureQuery = repository.Query()
                .Where(p => p.Salary > 65000)
                .OrderByDescending(p => p.Age);

            IEnumerable<Person> captureResult = await captureQuery.ExecuteAsync();
            List<Person> captureResultList = captureResult.ToList(); // Force enumeration to trigger SQL execution

            Assert.NotNull(repository.LastExecutedSql);
            Assert.Contains("WHERE", repository.LastExecutedSql);
            Assert.Contains("ORDER BY", repository.LastExecutedSql);

            // Clean up
            await repository.DeleteAllAsync();
        }

        /// <summary>
        /// Tests error handling and edge cases.
        /// </summary>
        [Fact]
        public async Task HandlesErrorsGracefully()
        {
            if (_SkipTests) return;

            SqlServerRepository<Person> repository = new SqlServerRepository<Person>(TestConnectionString);
            await SetupPersonTable(repository);

            // Test null parameter handling
            await Assert.ThrowsAsync<ArgumentNullException>(() => repository.CreateAsync(null));
            await Assert.ThrowsAsync<ArgumentNullException>(() => repository.ReadByIdAsync(null));

            // Test invalid ID
            Person nonExistent = await repository.ReadByIdAsync(99999);
            Assert.Null(nonExistent);

            // Note: ReadManyAsync(null) is valid - null predicate means "no filter"
        }

        /// <summary>
        /// Disposes test resources.
        /// </summary>
        public void Dispose()
        {
            // Test cleanup is handled per-test method
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
                using (SqlConnection connection = new SqlConnection(TestConnectionString))
                {
                    connection.Open();

                    using (Microsoft.Data.SqlClient.SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "SELECT 1";
                        command.ExecuteScalar();
                    }
                }

                Console.WriteLine("✅ SQL Server integration tests enabled - database connection successful");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  SQL Server integration tests disabled - could not connect to database: {ex.Message}");
                Console.WriteLine("To enable SQL Server integration tests:");
                Console.WriteLine("1. Start SQL Server on localhost:1433");
                Console.WriteLine($"2. Create database: CREATE DATABASE {TestDatabaseName};");
                Console.WriteLine("3. Create user: CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'test_password';");
                Console.WriteLine($"4. Grant permissions: GRANT ALL PRIVILEGES ON {TestDatabaseName}.* TO 'test_user'@'localhost';");
                _SkipTests = true;
            }
        }

        /// <summary>
        /// Creates or recreates the Person table for testing.
        /// </summary>
        /// <param name="repository">Repository to use for table creation</param>
        private async Task SetupPersonTable(SqlServerRepository<Person> repository)
        {
            // Drop table if exists
            try
            {
                await repository.ExecuteSqlAsync("IF OBJECT_ID('dbo.people', 'U') IS NOT NULL DROP TABLE people");
            }
            catch
            {
                // Table might not exist, ignore
            }

            // Create table
            await repository.ExecuteSqlAsync(@"
                CREATE TABLE people (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(128) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(32) NOT NULL
                )");
        }

        #endregion
    }
}