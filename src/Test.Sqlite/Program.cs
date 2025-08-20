namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;

    // Demo program
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== SQLite Repository Pattern Demo - Sync & Async ===\n");

            // For file-based database use: var connectionString = "Data Source=demo.db";
            // Create in-memory database
            var connectionString = "Data Source=InMemoryDemo;Mode=Memory;Cache=Shared";

            // Keep one connection open to maintain the in-memory database
            using var keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();

            InitializeDatabase(connectionString);

            // Create repository
            var repository = new SqliteRepository<Person>(connectionString);

            // Run synchronous tests
            Console.WriteLine("\n========== SYNCHRONOUS API TESTS ==========\n");
            TestSyncApi(repository);

            // Clear data before async tests
            repository.DeleteAll();

            // Run asynchronous tests
            Console.WriteLine("\n========== ASYNCHRONOUS API TESTS ==========\n");
            await TestAsyncApi(repository);

            // Run performance comparison
            Console.WriteLine("\n========== PERFORMANCE COMPARISON ==========\n");
            await TestPerformanceComparison(repository);

            // Test batch insert optimizations
            await BatchInsertTest.RunBatchInsertTests();

            // Run cancellation tests
            Console.WriteLine("\n========== CANCELLATION TOKEN TESTS ==========\n");
            await TestCancellation(repository);

            // Run enhanced transaction tests
            Console.WriteLine("\n========== ENHANCED TRANSACTION TESTS ==========\n");
            await TestTransactionsProperlyAsync(repository);

            // Run connection pooling demonstrations
            Console.WriteLine("\n========== CONNECTION POOLING TESTS ==========\n");
            PoolingExample.DemonstrateConnectionPooling();
            
            Console.WriteLine("\n");
            PoolingExample.ComparePerformance();

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        static void TestSyncApi(SqliteRepository<Person> repository)
        {
            // CREATE operations
            Console.WriteLine("--- SYNC CREATE OPERATIONS ---");
            var john = new Person
            {
                FirstName = "John",
                LastName = "Doe",
                Age = 30,
                Email = "john@example.com",
                Salary = 75000,
                Department = "IT"
            };
            john = repository.Create(john);
            Console.WriteLine($"Created: {john}");

            var people = GeneratePeople(10);
            var sw = Stopwatch.StartNew();
            var createdPeople = repository.CreateMany(people).ToList();
            sw.Stop();
            Console.WriteLine($"Created {createdPeople.Count} people in {sw.ElapsedMilliseconds}ms");

            // READ operations
            Console.WriteLine("\n--- SYNC READ OPERATIONS ---");

            // ReadFirst
            var first = repository.ReadFirst();
            Console.WriteLine($"ReadFirst: {first}");

            // Use == instead of Equals to avoid the method call issue
            var firstIT = repository.ReadFirst(p => p.Department == "IT");
            Console.WriteLine($"ReadFirst IT: {firstIT}");

            // ReadSingle
            try
            {
                var singleJohn = repository.ReadSingle(p => p.FirstName == "John");
                Console.WriteLine($"ReadSingle John: {singleJohn}");
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"ReadSingle error (expected if multiple Johns): {ex.Message}");
            }

            // ReadSingleOrDefault
            var singleOrDefault = repository.ReadSingleOrDefault(p => p.Email == "john@example.com");
            Console.WriteLine($"ReadSingleOrDefault: {singleOrDefault}");

            // ReadById
            var byId = repository.ReadById(1);
            Console.WriteLine($"ReadById(1): {byId}");

            // ReadMany
            var allIT = repository.ReadMany(p => p.Department == "IT").ToList();
            Console.WriteLine($"ReadMany IT: {allIT.Count} people");

            // ReadAll
            var all = repository.ReadAll().ToList();
            Console.WriteLine($"ReadAll: {all.Count} people");

            // EXISTS and COUNT operations
            Console.WriteLine("\n--- SYNC EXISTS AND COUNT OPERATIONS ---");
            var exists = repository.Exists(p => p.Age > 25);
            Console.WriteLine($"Exists age > 25: {exists}");

            var existsById = repository.ExistsById(1);
            Console.WriteLine($"ExistsById(1): {existsById}");

            var countIT = repository.Count(p => p.Department == "IT");
            Console.WriteLine($"Count IT: {countIT}");

            var totalCount = repository.Count();
            Console.WriteLine($"Total count: {totalCount}");

            // UPDATE operations
            Console.WriteLine("\n--- SYNC UPDATE OPERATIONS ---");
            john.Salary = 80000;
            repository.Update(john);
            Console.WriteLine($"Updated John's salary to 80000");

            var updatedFields = repository.UpdateField(
                p => p.Department == "IT",
                p => p.Salary,
                90000m
            );
            Console.WriteLine($"UpdateField: Updated {updatedFields} IT salaries to 90000");

            var updatedMany = repository.UpdateMany(
                p => p.Age > 30,
                person => person.Age += 1
            );
            Console.WriteLine($"UpdateMany: Incremented age for {updatedMany} people over 30");

            // QUERY BUILDER operations
            Console.WriteLine("\n--- SYNC QUERY BUILDER OPERATIONS ---");
            var query = repository.Query()
                .Where(p => p.Salary > 70000)
                .Where(p => p.Age < 40)
                .OrderByDescending(p => p.Salary)
                .ThenBy(p => p.LastName)
                .Skip(1)
                .Take(3);

            var queryResults = query.Execute().ToList();
            Console.WriteLine($"Complex query returned {queryResults.Count} results");
            foreach (var p in queryResults.Take(3))
            {
                Console.WriteLine($"  {p}");
            }

            var query1 = repository.Query()
                .Where(p => p.Department == "IT")
                .OrderByDescending(p => p.Salary)
                .Take(2);

            var queryResults1 = query1.Execute().ToList();
            Console.WriteLine($"\nTop 2 IT salaries:");
            foreach (var p in queryResults1) Console.WriteLine($"  {p}");

            var paginatedQuery = repository.Query()
                .Where(p => p.Salary > 60000)
                .OrderBy(p => p.LastName)
                .Skip(1)
                .Take(3);

            var paginatedResults = paginatedQuery.Execute().ToList();
            Console.WriteLine($"\nPaginated results:");
            foreach (var p in paginatedResults) Console.WriteLine($"  {p}");

            // Test Distinct
            repository.Create(new Person
            {
                FirstName = "Duplicate",
                LastName = "Test",
                Age = 25,
                Email = "dup1@test.com",
                Salary = 50000,
                Department = "HR"
            });
            repository.Create(new Person
            {
                FirstName = "Duplicate",
                LastName = "Test",
                Age = 25,
                Email = "dup2@test.com",
                Salary = 50000,
                Department = "HR"
            });

            var distinctCount = repository.Query()
                .Where(p => p.FirstName == "Duplicate")
                .Distinct()
                .Execute()
                .Count();
            Console.WriteLine($"Distinct 'Duplicate' count: {distinctCount}");

            // UPSERT operations
            Console.WriteLine("\n--- SYNC UPSERT OPERATIONS ---");
            var upsertPerson = new Person
            {
                Id = 1,
                FirstName = "John",
                LastName = "Updated",
                Age = 31,
                Email = "john.updated@example.com",
                Salary = 95000,
                Department = "Management"
            };
            repository.Upsert(upsertPerson);
            var afterUpsert = repository.ReadById(1);
            Console.WriteLine($"After upsert: {afterUpsert}");

            var upsertMany = new List<Person>
            {
                new Person { Id = 2, FirstName = "Jane", LastName = "UpsertTest", Age = 26, Email = "jane.new@test.com", Salary = 70000, Department = "IT" },
                new Person { Id = 999, FirstName = "New", LastName = "Person", Age = 35, Email = "new@test.com", Salary = 60000, Department = "Sales" }
            };
            var upsertedCount = repository.UpsertMany(upsertMany).Count();
            Console.WriteLine($"UpsertMany: Processed {upsertedCount} records");

            // DELETE operations
            Console.WriteLine("\n--- SYNC DELETE OPERATIONS ---");
            var toDeleteEntity = repository.ReadFirst(p => p.FirstName == "Duplicate");
            if (toDeleteEntity != null)
            {
                var deleted = repository.Delete(toDeleteEntity);
                Console.WriteLine($"Delete entity: {deleted}");
            }
            else
            {
                Console.WriteLine("No entity found to delete");
            }

            var deletedById = repository.DeleteById(999);
            Console.WriteLine($"DeleteById(999): {deletedById}");

            var deletedMany = repository.DeleteMany(p => p.Department == "Sales");
            Console.WriteLine($"DeleteMany Sales: {deletedMany} deleted");

            Console.WriteLine($"\nFinal sync count: {repository.Count()}");

            // AGGREGATE operations
            Console.WriteLine("\n--- SYNC AGGREGATE OPERATIONS ---");
            var maxSalary = repository.Max(p => p.Salary);
            Console.WriteLine($"Max salary: {maxSalary:C}");

            var minAge = repository.Min(p => p.Age);
            Console.WriteLine($"Min age: {minAge}");

            var avgSalary = repository.Average(p => p.Salary, p => p.Department == "IT");
            Console.WriteLine($"Average IT salary: {avgSalary:C}");

            var totalSalary = repository.Sum(p => p.Salary);
            Console.WriteLine($"Total salary: {totalSalary:C}");

            // BATCH operations
            Console.WriteLine("\n--- SYNC BATCH OPERATIONS ---");
            var batchDeleted = repository.BatchDelete(p => p.Age < 26);
            Console.WriteLine($"BatchDelete: Deleted {batchDeleted} young employees");

            // RAW SQL operations
            Console.WriteLine("\n--- SYNC RAW SQL OPERATIONS ---");
            var rawResults = repository.FromSql(
                "SELECT * FROM people WHERE age > @p0 AND department = @p1 ORDER BY salary DESC LIMIT 5",
                null,  // transaction parameter
                25, "IT"
            ).ToList();
            Console.WriteLine($"Raw SQL query returned {rawResults.Count} results");

            var affectedRows = repository.ExecuteSql(
                "UPDATE people SET salary = salary * @p0 WHERE department = @p1",
                null,  // transaction parameter
                1.05m, "Finance"
            );
            Console.WriteLine($"Raw SQL update affected {affectedRows} rows");

            // TRANSACTION operations
            Console.WriteLine("\n--- SYNC TRANSACTION OPERATIONS ---");
            var countBefore = repository.Count();
            Console.WriteLine($"Count before transaction: {countBefore}");

            using (var transaction = repository.BeginTransaction())
            {
                try
                {
                    // Note: We can't use repository methods during a transaction
                    // because they open new connections. This is a limitation
                    // of the current implementation.
                    Console.WriteLine("Transaction created (rollback test)");

                    // Rollback to test transaction
                    transaction.Rollback();
                    Console.WriteLine("Transaction rolled back");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Transaction error: {ex.Message}");
                    transaction.Rollback();
                }
            }

            // Test a commit transaction
            Console.WriteLine("\nTesting transaction with commit:");
            using (var transaction = repository.BeginTransaction())
            {
                try
                {
                    Console.WriteLine("Transaction created (commit test)");
                    transaction.Commit();
                    Console.WriteLine("Transaction committed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Transaction error: {ex.Message}");
                    transaction.Rollback();
                }
            }

            var finalCount = repository.Count();
            Console.WriteLine($"Count after transactions: {finalCount}");
        }

        static async Task TestAsyncApi(SqliteRepository<Person> repository)
        {
            // CREATE operations
            Console.WriteLine("--- ASYNC CREATE OPERATIONS ---");
            var john = new Person
            {
                FirstName = "John",
                LastName = "Async",
                Age = 30,
                Email = "john.async@example.com",
                Salary = 75000,
                Department = "IT"
            };
            john = await repository.CreateAsync(john);
            Console.WriteLine($"Created async: {john}");

            var people = GeneratePeople(20);
            var sw = Stopwatch.StartNew();
            var createdPeople = await repository.CreateManyAsync(people);
            sw.Stop();
            Console.WriteLine($"Created {createdPeople.Count()} people async in {sw.ElapsedMilliseconds}ms");

            // READ operations
            Console.WriteLine("\n--- ASYNC READ OPERATIONS ---");

            // ReadFirstAsync
            var first = await repository.ReadFirstAsync();
            Console.WriteLine($"ReadFirstAsync: {first}");

            var firstIT = await repository.ReadFirstAsync(p => p.Department == "IT");
            Console.WriteLine($"ReadFirstAsync IT: {firstIT}");

            // ReadSingleAsync
            try
            {
                var singleAsync = await repository.ReadSingleAsync(p => p.Email == "john.async@example.com");
                Console.WriteLine($"ReadSingleAsync: {singleAsync}");
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"ReadSingleAsync error: {ex.Message}");
            }

            // ReadSingleOrDefaultAsync
            var singleOrDefaultAsync = await repository.ReadSingleOrDefaultAsync(p => p.Email == "john.async@example.com");
            Console.WriteLine($"ReadSingleOrDefaultAsync: {singleOrDefaultAsync}");

            // ReadByIdAsync
            var byIdAsync = await repository.ReadByIdAsync(john.Id);
            Console.WriteLine($"ReadByIdAsync({john.Id}): {byIdAsync}");

            // ReadManyAsync with IAsyncEnumerable
            Console.WriteLine("ReadManyAsync IT department:");
            var count = 0;
            await foreach (var person in repository.ReadManyAsync(p => p.Department == "IT"))
            {
                count++;
                if (count <= 3) // Show first 3
                    Console.WriteLine($"  {person}");
            }
            Console.WriteLine($"  Total IT: {count}");

            // ReadAllAsync
            count = 0;
            await foreach (var person in repository.ReadAllAsync())
            {
                count++;
            }
            Console.WriteLine($"ReadAllAsync count: {count}");

            // EXISTS and COUNT operations
            Console.WriteLine("\n--- ASYNC EXISTS AND COUNT OPERATIONS ---");
            var existsAsync = await repository.ExistsAsync(p => p.Age > 25);
            Console.WriteLine($"ExistsAsync age > 25: {existsAsync}");

            var existsByIdAsync = await repository.ExistsByIdAsync(john.Id);
            Console.WriteLine($"ExistsByIdAsync({john.Id}): {existsByIdAsync}");

            var countITAsync = await repository.CountAsync(p => p.Department == "IT");
            Console.WriteLine($"CountAsync IT: {countITAsync}");

            var totalCountAsync = await repository.CountAsync();
            Console.WriteLine($"Total countAsync: {totalCountAsync}");

            // UPDATE operations
            Console.WriteLine("\n--- ASYNC UPDATE OPERATIONS ---");
            john.Salary = 85000;
            await repository.UpdateAsync(john);
            Console.WriteLine($"UpdateAsync: John's salary to 85000");

            var updatedFieldsAsync = await repository.UpdateFieldAsync(
                p => p.Department == "Finance",
                p => p.Salary,
                95000m
            );
            Console.WriteLine($"UpdateFieldAsync: Updated {updatedFieldsAsync} Finance salaries to 95000");

            // UpdateManyAsync with async action
            var updatedManyAsync = await repository.UpdateManyAsync(
                p => p.Age < 30,
                async person =>
                {
                    person.Age += 2;
                    // Simulate async work
                    await Task.Delay(1);
                }
            );
            Console.WriteLine($"UpdateManyAsync: Updated {updatedManyAsync} people under 30");

            // UPSERT operations
            Console.WriteLine("\n--- ASYNC UPSERT OPERATIONS ---");
            var upsertPersonAsync = new Person
            {
                Id = john.Id,
                FirstName = "John",
                LastName = "AsyncUpserted",
                Age = 32,
                Email = "john.upserted@example.com",
                Salary = 100000,
                Department = "Executive"
            };
            await repository.UpsertAsync(upsertPersonAsync);
            var afterUpsertAsync = await repository.ReadByIdAsync(john.Id);
            Console.WriteLine($"After upsertAsync: {afterUpsertAsync}");

            var upsertManyAsync = new List<Person>
            {
                new Person { Id = 1000, FirstName = "Async1", LastName = "Test", Age = 25, Email = "async1@test.com", Salary = 50000, Department = "QA" },
                new Person { Id = 1001, FirstName = "Async2", LastName = "Test", Age = 28, Email = "async2@test.com", Salary = 55000, Department = "QA" }
            };
            var upsertedCountAsync = (await repository.UpsertManyAsync(upsertManyAsync)).Count();
            Console.WriteLine($"UpsertManyAsync: Processed {upsertedCountAsync} records");

            // DELETE operations
            Console.WriteLine("\n--- ASYNC DELETE OPERATIONS ---");
            var toDelete = await repository.ReadFirstAsync(p => p.Department == "QA");
            if (toDelete != null)
            {
                var deletedAsync = await repository.DeleteAsync(toDelete);
                Console.WriteLine($"DeleteAsync entity: {deletedAsync}");
            }

            var deletedByIdAsync = await repository.DeleteByIdAsync(1001);
            Console.WriteLine($"DeleteByIdAsync(1001): {deletedByIdAsync}");

            var deletedManyAsync = await repository.DeleteManyAsync(p => p.Salary < 60000);
            Console.WriteLine($"DeleteManyAsync salary < 60000: {deletedManyAsync} deleted");

            Console.WriteLine($"\nFinal async count: {await repository.CountAsync()}");

            // AGGREGATE operations
            Console.WriteLine("\n--- ASYNC AGGREGATE OPERATIONS ---");
            var maxSalaryAsync = await repository.MaxAsync(p => p.Salary);
            Console.WriteLine($"Max salary: {maxSalaryAsync:C}");

            var minAgeAsync = await repository.MinAsync(p => p.Age);
            Console.WriteLine($"Min age: {minAgeAsync}");

            var avgSalaryAsync = await repository.AverageAsync(p => p.Salary, p => p.Department == "IT");
            Console.WriteLine($"Average IT salary: {avgSalaryAsync:C}");

            var totalSalaryAsync = await repository.SumAsync(p => p.Salary);
            Console.WriteLine($"Total salary: {totalSalaryAsync:C}");

            // BATCH operations
            Console.WriteLine("\n--- ASYNC BATCH OPERATIONS ---");
            var batchDeletedAsync = await repository.BatchDeleteAsync(p => p.Salary < 55000);
            Console.WriteLine($"BatchDeleteAsync: Deleted {batchDeletedAsync} low salary records");

            // RAW SQL operations
            Console.WriteLine("\n--- ASYNC RAW SQL OPERATIONS ---");
            var rawResultsCount = 0;
            await foreach (var person in repository.FromSqlAsync(
                "SELECT * FROM people WHERE age > @p0 ORDER BY salary DESC LIMIT 10",
                null,  // transaction parameter
                default(CancellationToken),
                30))
            {
                rawResultsCount++;
            }
            Console.WriteLine($"Raw SQL async query returned {rawResultsCount} results");

            var affectedRowsAsync = await repository.ExecuteSqlAsync(
                "UPDATE people SET age = age + @p0 WHERE department = @p1",
                null,  // transaction parameter
                default(CancellationToken),
                1, 
                "IT"
            );
            Console.WriteLine($"Raw SQL async update affected {affectedRowsAsync} rows");

            // TRANSACTION operations
            Console.WriteLine("\n--- ASYNC TRANSACTION OPERATIONS ---");
            var countBeforeAsync = await repository.CountAsync();
            Console.WriteLine($"Count before transaction: {countBeforeAsync}");

            using (var transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Note: We can't use repository methods during a transaction
                    // because they open new connections. This is a limitation
                    // of the current implementation.
                    Console.WriteLine("Async transaction created");

                    // Commit this time
                    await transaction.CommitAsync();
                    Console.WriteLine("Async transaction committed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Async transaction error: {ex.Message}");
                    await transaction.RollbackAsync();
                }
            }

            var countAfterAsync = await repository.CountAsync();
            Console.WriteLine($"Count after transaction: {countAfterAsync}");

            // Test query builder async execution
            Console.WriteLine("\n--- ASYNC QUERY BUILDER ---");
            var asyncQueryResults = await repository.Query()
                .Where(p => p.Department == "IT")
                .OrderByDescending(p => p.Salary)
                .ThenBy(p => p.Age)
                .Take(5)
                .ExecuteAsync();

            Console.WriteLine($"Async query builder returned {asyncQueryResults.Count()} results");

            // Test async enumerable query
            Console.WriteLine("\nStreaming results with ExecuteAsyncEnumerable:");
            var streamCount = 0;
            await foreach (var person in repository.Query()
                .Where(p => p.Salary > 60000)
                .OrderBy(p => p.LastName)
                .ExecuteAsyncEnumerable())
            {
                streamCount++;
                if (streamCount <= 3)
                {
                    Console.WriteLine($"  Streamed: {person}");
                }
            }
            Console.WriteLine($"  Total streamed: {streamCount}");
        }

        static async Task TestPerformanceComparison(SqliteRepository<Person> repository)
        {
            // Clear all data
            await repository.DeleteAllAsync();

            const int recordCount = 1000;
            var testData = GeneratePeople(recordCount);

            // Test sync performance
            var syncStopwatch = Stopwatch.StartNew();
            var syncCreated = repository.CreateMany(testData).ToList();
            syncStopwatch.Stop();
            Console.WriteLine($"Sync CreateMany {recordCount} records: {syncStopwatch.ElapsedMilliseconds}ms");

            // Clear for async test
            repository.DeleteAll();

            // Test async performance
            var asyncStopwatch = Stopwatch.StartNew();
            var asyncCreated = await repository.CreateManyAsync(testData);
            asyncStopwatch.Stop();
            Console.WriteLine($"Async CreateManyAsync {recordCount} records: {asyncStopwatch.ElapsedMilliseconds}ms");

            // Read performance comparison
            syncStopwatch.Restart();
            var syncReadCount = repository.ReadMany(p => p.Salary > 50000).Count();
            syncStopwatch.Stop();
            Console.WriteLine($"\nSync ReadMany count: {syncReadCount} in {syncStopwatch.ElapsedMilliseconds}ms");

            asyncStopwatch.Restart();
            var asyncReadCount = 0;
            await foreach (var person in repository.ReadManyAsync(p => p.Salary > 50000))
            {
                asyncReadCount++;
            }
            asyncStopwatch.Stop();
            Console.WriteLine($"Async ReadManyAsync count: {asyncReadCount} in {asyncStopwatch.ElapsedMilliseconds}ms");

            // Update performance comparison
            syncStopwatch.Restart();
            var syncUpdated = repository.UpdateField(p => p.Department == "IT", p => p.Salary, 100000m);
            syncStopwatch.Stop();
            Console.WriteLine($"\nSync UpdateField: {syncUpdated} records in {syncStopwatch.ElapsedMilliseconds}ms");

            asyncStopwatch.Restart();
            var asyncUpdated = await repository.UpdateFieldAsync(p => p.Department == "Finance", p => p.Salary, 100000m);
            asyncStopwatch.Stop();
            Console.WriteLine($"Async UpdateFieldAsync: {asyncUpdated} records in {asyncStopwatch.ElapsedMilliseconds}ms");

            // Cleanup
            await repository.DeleteAllAsync();
        }

        static async Task TestCancellation(SqliteRepository<Person> repository)
        {
            // Add test data
            var testData = GeneratePeople(100);
            await repository.CreateManyAsync(testData);

            // Test cancellation on read operation
            using (var cts = new CancellationTokenSource())
            {
                try
                {
                    var readTask = Task.Run(async () =>
                    {
                        var count = 0;
                        await foreach (var person in repository.ReadAllAsync(null, cts.Token))
                        {
                            count++;
                            if (count > 10)
                            {
                                cts.Cancel(); // Cancel after reading 10 records
                            }
                        }
                        return count;
                    });

                    await readTask;
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("ReadAllAsync was successfully cancelled");
                }
            }

            // Test pre-cancelled token
            using (var cts = new CancellationTokenSource())
            {
                cts.Cancel(); // Pre-cancel the token

                try
                {
                    await repository.CountAsync(p => p.Age > 20, null, cts.Token);
                    Console.WriteLine("ERROR: Operation should have been cancelled");
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("CountAsync with pre-cancelled token threw as expected");
                }
            }

            // Test cancellation during transaction
            using (var cts = new CancellationTokenSource(100)) // Cancel after 100ms
            {
                try
                {
                    var largeUpdate = GeneratePeople(500);
                    await repository.CreateManyAsync(largeUpdate, null, cts.Token);
                    Console.WriteLine("ERROR: Large operation should have been cancelled");
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("CreateManyAsync was cancelled during transaction");
                }
            }

            Console.WriteLine($"\nFinal count after cancellation tests: {await repository.CountAsync()}");
        }

        static List<Person> GeneratePeople(int count)
        {
            var departments = new[] { "IT", "HR", "Finance", "Sales", "Marketing", "Operations" };
            var random = new Random();
            var people = new List<Person>();

            for (int i = 0; i < count; i++)
            {
                people.Add(new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = random.Next(22, 65),
                    Email = $"person{i}@test.com",
                    Salary = random.Next(40000, 120000),
                    Department = departments[random.Next(departments.Length)]
                });
            }

            return people;
        }

        static void InitializeDatabase(string connectionString)
        {
            using var connection = new SqliteConnection(connectionString);
            connection.Open();

            // Create table
            var createTableSql = @"
                CREATE TABLE people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INTEGER NOT NULL,
                    email VARCHAR(128) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(32) NOT NULL
                );";

            using (var createCommand = new SqliteCommand(createTableSql, connection))
            {
                createCommand.ExecuteNonQuery();
            }

            Console.WriteLine("Database initialized.");
        }

        static async Task TestTransactionsProperlyAsync(IRepository<Person> repository)
        {
            Console.WriteLine("--- PROPER TRANSACTION TESTS ---");

            // Test 1: Rollback transaction
            Console.WriteLine("\nTest 1: Rollback Transaction");
            var countBefore = await repository.CountAsync();
            Console.WriteLine($"Count before transaction: {countBefore}");

            using (var transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Create person within transaction
                    var person1 = new Person
                    {
                        FirstName = "Transaction",
                        LastName = "Test1",
                        Age = 30,
                        Email = "trans1@test.com",
                        Salary = 75000,
                        Department = "IT"
                    };
                    var created1 = await repository.CreateAsync(person1, transaction);
                    Console.WriteLine($"Created in transaction: {created1}");

                    // Verify it exists within the transaction
                    var exists = await repository.ExistsByIdAsync(created1.Id, transaction);
                    Console.WriteLine($"Exists in transaction: {exists}");

                    // Count within transaction
                    var countInTransaction = await repository.CountAsync(null, transaction);
                    Console.WriteLine($"Count in transaction: {countInTransaction}");

                    // Rollback
                    await transaction.RollbackAsync();
                    Console.WriteLine("Transaction rolled back");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    await transaction.RollbackAsync();
                }
            }

            var countAfterRollback = await repository.CountAsync();
            Console.WriteLine($"Count after rollback: {countAfterRollback}");
            Console.WriteLine($"Rollback successful: {countAfterRollback == countBefore}");

            // Test 2: Commit transaction
            Console.WriteLine("\nTest 2: Commit Transaction");
            using (var transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Create multiple people in transaction
                    var people = new List<Person>
                    {
                        new Person { FirstName = "Trans", LastName = "Person1", Age = 25, Email = "tp1@test.com", Salary = 60000, Department = "HR" },
                        new Person { FirstName = "Trans", LastName = "Person2", Age = 28, Email = "tp2@test.com", Salary = 65000, Department = "HR" }
                    };

                    var created = await repository.CreateManyAsync(people, transaction);
                    Console.WriteLine($"Created {created.Count()} people in transaction");

                    // Update within transaction
                    var updated = await repository.UpdateFieldAsync(
                        p => p.Department == "HR",
                        p => p.Salary,
                        70000m,
                        transaction
                    );
                    Console.WriteLine($"Updated {updated} salaries in transaction");

                    // Query within transaction
                    var hrCount = await repository.CountAsync(p => p.Department == "HR", transaction);
                    Console.WriteLine($"HR count in transaction: {hrCount}");

                    // Commit
                    await transaction.CommitAsync();
                    Console.WriteLine("Transaction committed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    await transaction.RollbackAsync();
                    throw;
                }
            }

            var countAfterCommit = await repository.CountAsync();
            Console.WriteLine($"Count after commit: {countAfterCommit}");

            // Test 3: Raw SQL within transaction
            Console.WriteLine("\nTest 3: Raw SQL Within Transaction");
            using (var transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Execute raw SQL within transaction
                    var affectedRows = await repository.ExecuteSqlAsync(
                        "UPDATE people SET age = age + @p0 WHERE department = @p1",
                        transaction,
                        default(CancellationToken),
                        1, 
                        "HR"
                    );
                    Console.WriteLine($"Updated {affectedRows} ages using raw SQL");

                    // Query using raw SQL within transaction
                    var results = new List<Person>();
                    await foreach (var person in repository.FromSqlAsync(
                        "SELECT * FROM people WHERE department = @p0 ORDER BY salary DESC",
                        transaction,
                        default(CancellationToken),
                        "HR"))
                    {
                        results.Add(person);
                    }
                    Console.WriteLine($"Raw SQL query found {results.Count} results");

                    await transaction.CommitAsync();
                    Console.WriteLine("Transaction with raw SQL committed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    await transaction.RollbackAsync();
                }
            }

            // Test 4: Sync transaction operations
            Console.WriteLine("\nTest 4: Synchronous Transaction Operations");
            using (var transaction = repository.BeginTransaction())
            {
                try
                {
                    // Create
                    var person = new Person
                    {
                        FirstName = "Sync",
                        LastName = "Transaction",
                        Age = 40,
                        Email = "sync@test.com",
                        Salary = 85000,
                        Department = "IT"
                    };
                    repository.Create(person, transaction);

                    // Update
                    person.Salary = 90000;
                    repository.Update(person, transaction);

                    // Delete some records
                    var deleted = repository.DeleteMany(p => p.Age > 65, transaction);
                    Console.WriteLine($"Deleted {deleted} old records");

                    transaction.Commit();
                    Console.WriteLine("Sync transaction committed");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    transaction.Rollback();
                }
            }

            Console.WriteLine("\nTransaction tests completed");
        }
    }
}