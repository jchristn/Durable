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
        private static readonly List<TestResult> _testResults = new List<TestResult>();

        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Starting program...");
                Console.WriteLine("=== SQLite Repository Pattern Demo - Sync & Async ===\n");

            // For file-based database use: var connectionString = "Data Source=demo.db";
            // Create in-memory database
            string connectionString = "Data Source=InMemoryDemo;Mode=Memory;Cache=Shared";

            // Keep one connection open to maintain the in-memory database
            using SqliteConnection keepAliveConnection = new SqliteConnection(connectionString);
            keepAliveConnection.Open();

            InitializeDatabase(connectionString);

            // Create repository
            SqliteRepository<Person> repository = new SqliteRepository<Person>(connectionString);

            // Run synchronous tests
            Console.WriteLine("========== SYNCHRONOUS API TESTS ==========");
            await RunTest("Synchronous API", () => TestSyncApi(repository));

            // Clear data before async tests
            repository.DeleteAll();

            // Run asynchronous tests
            Console.WriteLine("\n========== ASYNCHRONOUS API TESTS ==========");
            await RunTestAsync("Asynchronous API", () => TestAsyncApi(repository));


            // Test batch insert optimizations
            Console.WriteLine("\n========== BATCH INSERT TESTS ==========");
            await RunTestAsync("Batch Insert", () => BatchInsertTest.RunBatchInsertTests());

            // Run cancellation tests
            Console.WriteLine("\n========== CANCELLATION TOKEN TESTS ==========");
            await RunTestAsync("Cancellation Token", () => TestCancellation(repository));

            // Run enhanced transaction tests
            Console.WriteLine("\n========== ENHANCED TRANSACTION TESTS ==========");
            await RunTestAsync("Enhanced Transaction", () => TestTransactionsProperlyAsync(repository));

            // Run connection pooling demonstrations
            Console.WriteLine("\n========== CONNECTION POOLING TESTS ==========");
            await RunTest("Connection Pooling", () => 
            {
                PoolingExample.DemonstrateConnectionPooling();
            });

            // Run query exposure tests
            Console.WriteLine("\n========== QUERY EXPOSURE TESTS ==========");
            await RunTestAsync("Query Exposure", () => QueryExposureTest.RunQueryExposureTest());

            // Run complex expression tests
            Console.WriteLine("\n========== COMPLEX EXPRESSION TESTS ==========");
            await RunTest("Complex Expression Parsing", () => 
            {
                ComplexExpressionTest complexTest = new ComplexExpressionTest();
                complexTest.RunAllTests();
            });

            // Run sanitization tests
            Console.WriteLine("\n========== SANITIZATION TESTS ==========");
            await RunTestAsync("SQL Injection Protection", () => SanitizationTest.RunSanitizationTests());

            // Run data type converter tests
            Console.WriteLine("\n========== DATA TYPE CONVERTER TESTS ==========");
            await RunTestAsync("Data Type Converter", () => DataTypeConverterTest.RunDataTypeConverterTest());

            // Run transaction scope tests
            Console.WriteLine("\n========== TRANSACTION SCOPE TESTS ==========");
            await RunTest("Transaction Scope", () => 
            {
                TransactionScopeTest transactionTest = new TransactionScopeTest();
                transactionTest.RunAllTests();
                transactionTest.Dispose();
            });

            // Display summary
            DisplayTestSummary();

            Console.WriteLine("\nProgram completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Program failed with exception: {ex}");
                Console.WriteLine("\nProgram terminated with errors.");
            }
        }

        static void TestSyncApi(SqliteRepository<Person> repository)
        {
            // CREATE operations
            Console.WriteLine("--- SYNC CREATE OPERATIONS ---");
            Person john = new Person
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

            List<Person> people = GeneratePeople(10);
            List<Person> createdPeople = repository.CreateMany(people).ToList();
            Console.WriteLine($"Created {createdPeople.Count} people");

            // READ operations
            Console.WriteLine("\n--- SYNC READ OPERATIONS ---");

            // ReadFirst
            Person first = repository.ReadFirst();
            Console.WriteLine($"ReadFirst: {first}");

            // Use == instead of Equals to avoid the method call issue
            Person firstIT = repository.ReadFirst(p => p.Department == "IT");
            Console.WriteLine($"ReadFirst IT: {firstIT}");

            // ReadSingle
            try
            {
                Person singleJohn = repository.ReadSingle(p => p.FirstName == "John");
                Console.WriteLine($"ReadSingle John: {singleJohn}");
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"ReadSingle error (expected if multiple Johns): {ex.Message}");
            }

            // ReadSingleOrDefault
            Person singleOrDefault = repository.ReadSingleOrDefault(p => p.Email == "john@example.com");
            Console.WriteLine($"ReadSingleOrDefault: {singleOrDefault}");

            // ReadById
            Person byId = repository.ReadById(1);
            Console.WriteLine($"ReadById(1): {byId}");

            // ReadMany
            List<Person> allIT = repository.ReadMany(p => p.Department == "IT").ToList();
            Console.WriteLine($"ReadMany IT: {allIT.Count} people");

            // ReadAll
            List<Person> all = repository.ReadAll().ToList();
            Console.WriteLine($"ReadAll: {all.Count} people");

            // EXISTS and COUNT operations
            Console.WriteLine("\n--- SYNC EXISTS AND COUNT OPERATIONS ---");
            bool exists = repository.Exists(p => p.Age > 25);
            Console.WriteLine($"Exists age > 25: {exists}");

            bool existsById = repository.ExistsById(1);
            Console.WriteLine($"ExistsById(1): {existsById}");

            int countIT = repository.Count(p => p.Department == "IT");
            Console.WriteLine($"Count IT: {countIT}");

            int totalCount = repository.Count();
            Console.WriteLine($"Total count: {totalCount}");

            // UPDATE operations
            Console.WriteLine("\n--- SYNC UPDATE OPERATIONS ---");
            john.Salary = 80000;
            repository.Update(john);
            Console.WriteLine($"Updated John's salary to 80000");

            int updatedFields = repository.UpdateField(
                p => p.Department == "IT",
                p => p.Salary,
                90000m
            );
            Console.WriteLine($"UpdateField: Updated {updatedFields} IT salaries to 90000");

            int updatedMany = repository.UpdateMany(
                p => p.Age > 30,
                person => person.Age += 1
            );
            Console.WriteLine($"UpdateMany: Incremented age for {updatedMany} people over 30");

            // QUERY BUILDER operations
            Console.WriteLine("\n--- SYNC QUERY BUILDER OPERATIONS ---");
            IQueryBuilder<Person> query = repository.Query()
                .Where(p => p.Salary > 70000)
                .Where(p => p.Age < 40)
                .OrderByDescending(p => p.Salary)
                .ThenBy(p => p.LastName)
                .Skip(1)
                .Take(3);

            List<Person> queryResults = query.Execute().ToList();
            Console.WriteLine($"Complex query returned {queryResults.Count} results");
            foreach (Person p in queryResults.Take(3))
            {
                Console.WriteLine($"  {p}");
            }

            // RAW QUERY EXPOSURE operations
            Console.WriteLine("\n--- SYNC RAW QUERY EXPOSURE ---");
            
            // Test ExecuteWithQuery - returns both query and results
            IDurableResult<Person> queryWithResults = repository.Query()
                .Where(p => p.Department == "IT")
                .Where(p => p.Salary > 60000)
                .ExecuteWithQuery();
            Console.WriteLine($"ExecuteWithQuery SQL: {queryWithResults.Query}");
            Console.WriteLine($"ExecuteWithQuery results: {queryWithResults.Result.Count()} records");
            
            // Test SelectWithQuery extension method
            IDurableResult<Person> extensionResult = repository.SelectWithQuery(p => p.Age > 25 && p.Salary < 100000);
            Console.WriteLine($"SelectWithQuery SQL: {extensionResult.Query}");
            Console.WriteLine($"SelectWithQuery results: {extensionResult.Result.Count()} records");
            
            // Test GetSelectQuery - returns only the SQL query
            string sqlOnly = repository.GetSelectQuery(p => p.FirstName.Contains("o") || p.LastName.StartsWith("D"));
            Console.WriteLine($"GetSelectQuery SQL only: {sqlOnly}");

            IQueryBuilder<Person> query1 = repository.Query()
                .Where(p => p.Department == "IT")
                .OrderByDescending(p => p.Salary)
                .Take(2);

            List<Person> queryResults1 = query1.Execute().ToList();
            Console.WriteLine($"\nTop 2 IT salaries:");
            foreach (Person p in queryResults1) Console.WriteLine($"  {p}");

            IQueryBuilder<Person> paginatedQuery = repository.Query()
                .Where(p => p.Salary > 60000)
                .OrderBy(p => p.LastName)
                .Skip(1)
                .Take(3);

            List<Person> paginatedResults = paginatedQuery.Execute().ToList();
            Console.WriteLine($"\nPaginated results:");
            foreach (Person p in paginatedResults) Console.WriteLine($"  {p}");

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

            int distinctCount = repository.Query()
                .Where(p => p.FirstName == "Duplicate")
                .Distinct()
                .Execute()
                .Count();
            Console.WriteLine($"Distinct 'Duplicate' count: {distinctCount}");

            // UPSERT operations
            Console.WriteLine("\n--- SYNC UPSERT OPERATIONS ---");
            Person upsertPerson = new Person
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
            Person afterUpsert = repository.ReadById(1);
            Console.WriteLine($"After upsert: {afterUpsert}");

            List<Person> upsertMany = new List<Person>
            {
                new Person { Id = 2, FirstName = "Jane", LastName = "UpsertTest", Age = 26, Email = "jane.new@test.com", Salary = 70000, Department = "IT" },
                new Person { Id = 999, FirstName = "New", LastName = "Person", Age = 35, Email = "new@test.com", Salary = 60000, Department = "Sales" }
            };
            int upsertedCount = repository.UpsertMany(upsertMany).Count();
            Console.WriteLine($"UpsertMany: Processed {upsertedCount} records");

            // DELETE operations
            Console.WriteLine("\n--- SYNC DELETE OPERATIONS ---");
            Person toDeleteEntity = repository.ReadFirst(p => p.FirstName == "Duplicate");
            if (toDeleteEntity != null)
            {
                bool deleted = repository.Delete(toDeleteEntity);
                Console.WriteLine($"Delete entity: {deleted}");
            }
            else
            {
                Console.WriteLine("No entity found to delete");
            }

            bool deletedById = repository.DeleteById(999);
            Console.WriteLine($"DeleteById(999): {deletedById}");

            int deletedMany = repository.DeleteMany(p => p.Department == "Sales");
            Console.WriteLine($"DeleteMany Sales: {deletedMany} deleted");

            Console.WriteLine($"\nFinal sync count: {repository.Count()}");

            // AGGREGATE operations
            Console.WriteLine("\n--- SYNC AGGREGATE OPERATIONS ---");
            decimal maxSalary = repository.Max(p => p.Salary);
            Console.WriteLine($"Max salary: {maxSalary:C}");

            int minAge = repository.Min(p => p.Age);
            Console.WriteLine($"Min age: {minAge}");

            decimal avgSalary = repository.Average(p => p.Salary, p => p.Department == "IT");
            Console.WriteLine($"Average IT salary: {avgSalary:C}");

            decimal totalSalary = repository.Sum(p => p.Salary);
            Console.WriteLine($"Total salary: {totalSalary:C}");

            // BATCH operations
            Console.WriteLine("\n--- SYNC BATCH OPERATIONS ---");
            int batchDeleted = repository.BatchDelete(p => p.Age < 26);
            Console.WriteLine($"BatchDelete: Deleted {batchDeleted} young employees");

            // RAW SQL operations
            Console.WriteLine("\n--- SYNC RAW SQL OPERATIONS ---");
            List<Person> rawResults = repository.FromSql(
                "SELECT * FROM people WHERE age > @p0 AND department = @p1 ORDER BY salary DESC LIMIT 5",
                null,  // transaction parameter
                25, "IT"
            ).ToList();
            Console.WriteLine($"Raw SQL query returned {rawResults.Count} results");

            int affectedRows = repository.ExecuteSql(
                "UPDATE people SET salary = salary * @p0 WHERE department = @p1",
                null,  // transaction parameter
                1.05m, "Finance"
            );
            Console.WriteLine($"Raw SQL update affected {affectedRows} rows");

            // TRANSACTION operations
            Console.WriteLine("\n--- SYNC TRANSACTION OPERATIONS ---");
            int countBefore = repository.Count();
            Console.WriteLine($"Count before transaction: {countBefore}");

            using (ITransaction transaction = repository.BeginTransaction())
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
            using (ITransaction transaction = repository.BeginTransaction())
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

            int finalCount = repository.Count();
            Console.WriteLine($"Count after transactions: {finalCount}");
        }

        static async Task TestAsyncApi(SqliteRepository<Person> repository)
        {
            // CREATE operations
            Console.WriteLine("--- ASYNC CREATE OPERATIONS ---");
            Person john = new Person
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

            List<Person> people = GeneratePeople(20);
            IEnumerable<Person> createdPeople = await repository.CreateManyAsync(people);
            Console.WriteLine($"Created {createdPeople.Count()} people async");

            // READ operations
            Console.WriteLine("\n--- ASYNC READ OPERATIONS ---");

            // ReadFirstAsync
            Person first = await repository.ReadFirstAsync();
            Console.WriteLine($"ReadFirstAsync: {first}");

            Person firstIT = await repository.ReadFirstAsync(p => p.Department == "IT");
            Console.WriteLine($"ReadFirstAsync IT: {firstIT}");

            // ReadSingleAsync
            try
            {
                Person singleAsync = await repository.ReadSingleAsync(p => p.Email == "john.async@example.com");
                Console.WriteLine($"ReadSingleAsync: {singleAsync}");
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"ReadSingleAsync error: {ex.Message}");
            }

            // ReadSingleOrDefaultAsync
            Person singleOrDefaultAsync = await repository.ReadSingleOrDefaultAsync(p => p.Email == "john.async@example.com");
            Console.WriteLine($"ReadSingleOrDefaultAsync: {singleOrDefaultAsync}");

            // ReadByIdAsync
            Person byIdAsync = await repository.ReadByIdAsync(john.Id);
            Console.WriteLine($"ReadByIdAsync({john.Id}): {byIdAsync}");

            // ReadManyAsync with IAsyncEnumerable
            Console.WriteLine("ReadManyAsync IT department:");
            int count = 0;
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == "IT"))
            {
                count++;
                if (count <= 3) // Show first 3
                    Console.WriteLine($"  {person}");
            }
            Console.WriteLine($"  Total IT: {count}");

            // ReadAllAsync
            count = 0;
            await foreach (Person person in repository.ReadAllAsync())
            {
                count++;
            }
            Console.WriteLine($"ReadAllAsync count: {count}");

            // EXISTS and COUNT operations
            Console.WriteLine("\n--- ASYNC EXISTS AND COUNT OPERATIONS ---");
            bool existsAsync = await repository.ExistsAsync(p => p.Age > 25);
            Console.WriteLine($"ExistsAsync age > 25: {existsAsync}");

            bool existsByIdAsync = await repository.ExistsByIdAsync(john.Id);
            Console.WriteLine($"ExistsByIdAsync({john.Id}): {existsByIdAsync}");

            int countITAsync = await repository.CountAsync(p => p.Department == "IT");
            Console.WriteLine($"CountAsync IT: {countITAsync}");

            int totalCountAsync = await repository.CountAsync();
            Console.WriteLine($"Total countAsync: {totalCountAsync}");

            // UPDATE operations
            Console.WriteLine("\n--- ASYNC UPDATE OPERATIONS ---");
            john.Salary = 85000;
            await repository.UpdateAsync(john);
            Console.WriteLine($"UpdateAsync: John's salary to 85000");

            int updatedFieldsAsync = await repository.UpdateFieldAsync(
                p => p.Department == "Finance",
                p => p.Salary,
                95000m
            );
            Console.WriteLine($"UpdateFieldAsync: Updated {updatedFieldsAsync} Finance salaries to 95000");

            // UpdateManyAsync with async action
            int updatedManyAsync = await repository.UpdateManyAsync(
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
            Person upsertPersonAsync = new Person
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
            Person afterUpsertAsync = await repository.ReadByIdAsync(john.Id);
            Console.WriteLine($"After upsertAsync: {afterUpsertAsync}");

            List<Person> upsertManyAsync = new List<Person>
            {
                new Person { Id = 1000, FirstName = "Async1", LastName = "Test", Age = 25, Email = "async1@test.com", Salary = 50000, Department = "QA" },
                new Person { Id = 1001, FirstName = "Async2", LastName = "Test", Age = 28, Email = "async2@test.com", Salary = 55000, Department = "QA" }
            };
            int upsertedCountAsync = (await repository.UpsertManyAsync(upsertManyAsync)).Count();
            Console.WriteLine($"UpsertManyAsync: Processed {upsertedCountAsync} records");

            // DELETE operations
            Console.WriteLine("\n--- ASYNC DELETE OPERATIONS ---");
            Person toDelete = await repository.ReadFirstAsync(p => p.Department == "QA");
            if (toDelete != null)
            {
                bool deletedAsync = await repository.DeleteAsync(toDelete);
                Console.WriteLine($"DeleteAsync entity: {deletedAsync}");
            }

            bool deletedByIdAsync = await repository.DeleteByIdAsync(1001);
            Console.WriteLine($"DeleteByIdAsync(1001): {deletedByIdAsync}");

            int deletedManyAsync = await repository.DeleteManyAsync(p => p.Salary < 60000);
            Console.WriteLine($"DeleteManyAsync salary < 60000: {deletedManyAsync} deleted");

            Console.WriteLine($"\nFinal async count: {await repository.CountAsync()}");

            // AGGREGATE operations
            Console.WriteLine("\n--- ASYNC AGGREGATE OPERATIONS ---");
            decimal maxSalaryAsync = await repository.MaxAsync(p => p.Salary);
            Console.WriteLine($"Max salary: {maxSalaryAsync:C}");

            int minAgeAsync = await repository.MinAsync(p => p.Age);
            Console.WriteLine($"Min age: {minAgeAsync}");

            decimal avgSalaryAsync = await repository.AverageAsync(p => p.Salary, p => p.Department == "IT");
            Console.WriteLine($"Average IT salary: {avgSalaryAsync:C}");

            decimal totalSalaryAsync = await repository.SumAsync(p => p.Salary);
            Console.WriteLine($"Total salary: {totalSalaryAsync:C}");

            // BATCH operations
            Console.WriteLine("\n--- ASYNC BATCH OPERATIONS ---");
            int batchDeletedAsync = await repository.BatchDeleteAsync(p => p.Salary < 55000);
            Console.WriteLine($"BatchDeleteAsync: Deleted {batchDeletedAsync} low salary records");

            // RAW SQL operations
            Console.WriteLine("\n--- ASYNC RAW SQL OPERATIONS ---");
            int rawResultsCount = 0;
            await foreach (Person person in repository.FromSqlAsync(
                "SELECT * FROM people WHERE age > @p0 ORDER BY salary DESC LIMIT 10",
                null,  // transaction parameter
                default(CancellationToken),
                30))
            {
                rawResultsCount++;
            }
            Console.WriteLine($"Raw SQL async query returned {rawResultsCount} results");

            int affectedRowsAsync = await repository.ExecuteSqlAsync(
                "UPDATE people SET age = age + @p0 WHERE department = @p1",
                null,  // transaction parameter
                default(CancellationToken),
                1, 
                "IT"
            );
            Console.WriteLine($"Raw SQL async update affected {affectedRowsAsync} rows");

            // TRANSACTION operations
            Console.WriteLine("\n--- ASYNC TRANSACTION OPERATIONS ---");
            int countBeforeAsync = await repository.CountAsync();
            Console.WriteLine($"Count before transaction: {countBeforeAsync}");

            using (ITransaction transaction = await repository.BeginTransactionAsync())
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

            int countAfterAsync = await repository.CountAsync();
            Console.WriteLine($"Count after transaction: {countAfterAsync}");

            // Test query builder async execution
            Console.WriteLine("\n--- ASYNC QUERY BUILDER ---");
            IEnumerable<Person> asyncQueryResults = await repository.Query()
                .Where(p => p.Department == "IT")
                .OrderByDescending(p => p.Salary)
                .ThenBy(p => p.Age)
                .Take(5)
                .ExecuteAsync();

            Console.WriteLine($"Async query builder returned {asyncQueryResults.Count()} results");

            // Test async enumerable query
            Console.WriteLine("\nStreaming results with ExecuteAsyncEnumerable:");
            int streamCount = 0;
            await foreach (Person person in repository.Query()
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

            // RAW QUERY EXPOSURE operations (Async)
            Console.WriteLine("\n--- ASYNC RAW QUERY EXPOSURE ---");
            
            // Test ExecuteWithQueryAsync - returns both query and results
            IDurableResult<Person> asyncQueryWithResults = await repository.Query()
                .Where(p => p.Department == "IT")
                .Where(p => p.Age > 25)
                .ExecuteWithQueryAsync();
            Console.WriteLine($"ExecuteWithQueryAsync SQL: {asyncQueryWithResults.Query}");
            Console.WriteLine($"ExecuteWithQueryAsync results: {asyncQueryWithResults.Result.Count()} records");
            
            // Test SelectWithQueryAsync extension method
            IDurableResult<Person> asyncExtensionResult = await repository.SelectWithQueryAsync(p => p.Salary > 70000 && p.Department != "Sales");
            Console.WriteLine($"SelectWithQueryAsync SQL: {asyncExtensionResult.Query}");
            Console.WriteLine($"SelectWithQueryAsync results: {asyncExtensionResult.Result.Count()} records");
            
            // Test SelectAsyncWithQuery - streaming with query
            Console.WriteLine("\nTesting SelectAsyncWithQuery (streaming):");
            IAsyncDurableResult<Person> streamingWithQuery = repository.SelectAsyncWithQuery(p => p.Age < 35);
            Console.WriteLine($"SelectAsyncWithQuery SQL: {streamingWithQuery.Query}");
            int streamingCount = 0;
            await foreach (Person person in streamingWithQuery.Result)
            {
                streamingCount++;
                if (streamingCount <= 2)
                {
                    Console.WriteLine($"  Streaming result: {person.FirstName} {person.LastName}");
                }
            }
            Console.WriteLine($"  Total streaming results: {streamingCount}");
        }


        static async Task TestCancellation(SqliteRepository<Person> repository)
        {
            // Add test data
            List<Person> testData = GeneratePeople(100);
            await repository.CreateManyAsync(testData);

            // Test cancellation on read operation
            using (CancellationTokenSource cts = new CancellationTokenSource())
            {
                try
                {
                    Task<int> readTask = Task.Run(async () =>
                    {
                        int count = 0;
                        await foreach (Person person in repository.ReadAllAsync(null, cts.Token))
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
            using (CancellationTokenSource cts = new CancellationTokenSource())
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
            using (CancellationTokenSource cts = new CancellationTokenSource(100)) // Cancel after 100ms
            {
                try
                {
                    List<Person> largeUpdate = GeneratePeople(500);
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
            string[] departments = new[] { "IT", "HR", "Finance", "Sales", "Marketing", "Operations" };
            Random random = new Random();
            List<Person> people = new List<Person>();

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
            using SqliteConnection connection = new SqliteConnection(connectionString);
            connection.Open();

            // Create table
            string createTableSql = @"
                CREATE TABLE people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first VARCHAR(64) NOT NULL,
                    last VARCHAR(64) NOT NULL,
                    age INTEGER NOT NULL,
                    email VARCHAR(128) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(32) NOT NULL
                );";

            using (SqliteCommand createCommand = new SqliteCommand(createTableSql, connection))
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
            int countBefore = await repository.CountAsync();
            Console.WriteLine($"Count before transaction: {countBefore}");

            using (ITransaction transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Create person within transaction
                    Person person1 = new Person
                    {
                        FirstName = "Transaction",
                        LastName = "Test1",
                        Age = 30,
                        Email = "trans1@test.com",
                        Salary = 75000,
                        Department = "IT"
                    };
                    Person created1 = await repository.CreateAsync(person1, transaction);
                    Console.WriteLine($"Created in transaction: {created1}");

                    // Verify it exists within the transaction
                    bool exists = await repository.ExistsByIdAsync(created1.Id, transaction);
                    Console.WriteLine($"Exists in transaction: {exists}");

                    // Count within transaction
                    int countInTransaction = await repository.CountAsync(null, transaction);
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

            int countAfterRollback = await repository.CountAsync();
            Console.WriteLine($"Count after rollback: {countAfterRollback}");
            Console.WriteLine($"Rollback successful: {countAfterRollback == countBefore}");

            // Test 2: Commit transaction
            Console.WriteLine("\nTest 2: Commit Transaction");
            using (ITransaction transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Create multiple people in transaction
                    List<Person> people = new List<Person>
                    {
                        new Person { FirstName = "Trans", LastName = "Person1", Age = 25, Email = "tp1@test.com", Salary = 60000, Department = "HR" },
                        new Person { FirstName = "Trans", LastName = "Person2", Age = 28, Email = "tp2@test.com", Salary = 65000, Department = "HR" }
                    };

                    IEnumerable<Person> created = await repository.CreateManyAsync(people, transaction);
                    Console.WriteLine($"Created {created.Count()} people in transaction");

                    // Update within transaction
                    int updated = await repository.UpdateFieldAsync(
                        p => p.Department == "HR",
                        p => p.Salary,
                        70000m,
                        transaction
                    );
                    Console.WriteLine($"Updated {updated} salaries in transaction");

                    // Query within transaction
                    int hrCount = await repository.CountAsync(p => p.Department == "HR", transaction);
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

            int countAfterCommit = await repository.CountAsync();
            Console.WriteLine($"Count after commit: {countAfterCommit}");

            // Test 3: Raw SQL within transaction
            Console.WriteLine("\nTest 3: Raw SQL Within Transaction");
            using (ITransaction transaction = await repository.BeginTransactionAsync())
            {
                try
                {
                    // Execute raw SQL within transaction
                    int affectedRows = await repository.ExecuteSqlAsync(
                        "UPDATE people SET age = age + @p0 WHERE department = @p1",
                        transaction,
                        default(CancellationToken),
                        1, 
                        "HR"
                    );
                    Console.WriteLine($"Updated {affectedRows} ages using raw SQL");

                    // Query using raw SQL within transaction
                    List<Person> results = new List<Person>();
                    await foreach (Person person in repository.FromSqlAsync(
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
            using (ITransaction transaction = repository.BeginTransaction())
            {
                try
                {
                    // Create
                    Person person = new Person
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
                    int deleted = repository.DeleteMany(p => p.Age > 65, transaction);
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

        static Task RunTest(string testName, Action testAction)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                testAction();
                stopwatch.Stop();
                _testResults.Add(new TestResult(testName, true, stopwatch.ElapsedMilliseconds, null));
                Console.WriteLine($"✅ {testName} completed in {stopwatch.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _testResults.Add(new TestResult(testName, false, stopwatch.ElapsedMilliseconds, ex.Message));
                Console.WriteLine($"❌ {testName} failed in {stopwatch.ElapsedMilliseconds}ms: {ex.Message}");
            }
            return Task.CompletedTask;
        }

        static async Task RunTestAsync(string testName, Func<Task> testAction)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                await testAction();
                stopwatch.Stop();
                _testResults.Add(new TestResult(testName, true, stopwatch.ElapsedMilliseconds, null));
                Console.WriteLine($"✅ {testName} completed in {stopwatch.ElapsedMilliseconds}ms");
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                _testResults.Add(new TestResult(testName, false, stopwatch.ElapsedMilliseconds, ex.Message));
                Console.WriteLine($"❌ {testName} failed in {stopwatch.ElapsedMilliseconds}ms: {ex.Message}");
            }
        }

        static void DisplayTestSummary()
        {
            Console.WriteLine("\n" + new string('=', 60));
            Console.WriteLine("TEST RESULTS SUMMARY");
            Console.WriteLine(new string('=', 60));

            List<TestResult> passedTests = _testResults.Where(r => r.Success).ToList();
            List<TestResult> failedTests = _testResults.Where(r => !r.Success).ToList();
            long totalTime = _testResults.Sum(r => r.ElapsedMs);

            Console.WriteLine($"Total Tests: {_testResults.Count}");
            Console.WriteLine($"Passed: {passedTests.Count} ✅");
            Console.WriteLine($"Failed: {failedTests.Count} ❌");
            Console.WriteLine($"Total Execution Time: {totalTime}ms");
            Console.WriteLine($"Success Rate: {(double)passedTests.Count / _testResults.Count * 100:F1}%");

            if (failedTests.Any())
            {
                Console.WriteLine("\nFailed Tests:");
                foreach (TestResult test in failedTests)
                {
                    Console.WriteLine($"  ❌ {test.Name}: {test.ErrorMessage}");
                }
            }

            Console.WriteLine("\nAll Tests:");
            foreach (TestResult test in _testResults)
            {
                string status = test.Success ? "✅" : "❌";
                Console.WriteLine($"  {status} {test.Name} ({test.ElapsedMs}ms)");
            }
        }
    }

    public record TestResult(string Name, bool Success, long ElapsedMs, string ErrorMessage);
}