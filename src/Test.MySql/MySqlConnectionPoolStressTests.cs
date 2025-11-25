namespace Test.MySql
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// MySQL-specific connection pool stress tests.
    /// MySQL supports the SLEEP() function for simulating slow queries.
    /// </summary>
    [Collection("MySQL Database Collection")]
    public class MySqlConnectionPoolStressTests : ConnectionPoolStressTests
    {
        #region Private-Members

        private readonly MySqlRepositoryProvider _Provider;
        private readonly string _ConnectionString;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlConnectionPoolStressTests"/> class.
        /// </summary>
        public MySqlConnectionPoolStressTests()
            : base(
                new MySqlRepositoryProvider(
                    Environment.GetEnvironmentVariable("MYSQL_CONNECTION_STRING")
                    ?? "Server=localhost;Database=durable_test;User=test_user;Password=test_password;"),
                "SELECT SLEEP(1)",
                "SELECT SLEEP({0})")
        {
            _ConnectionString = Environment.GetEnvironmentVariable("MYSQL_CONNECTION_STRING")
                ?? "Server=localhost;Database=durable_test;User=test_user;Password=test_password;";
            _Provider = new MySqlRepositoryProvider(_ConnectionString);
        }

        #endregion

        #region Abstract-Implementation

        /// <inheritdoc/>
        protected override IConnectionFactory CreateConnectionFactory(ConnectionPoolOptions options)
        {
            return new MySqlConnectionFactory(_ConnectionString, options);
        }

        /// <inheritdoc/>
        protected override IRepository<T> CreateRepository<T>(IConnectionFactory factory)
        {
            return new MySqlRepository<T>((MySqlConnectionFactory)factory);
        }

        /// <inheritdoc/>
        protected override async Task<bool> IsDatabaseAvailableAsync()
        {
            return await _Provider.IsDatabaseAvailableAsync();
        }

        /// <inheritdoc/>
        protected override async Task SetupDatabaseAsync()
        {
            await _Provider.SetupDatabaseAsync();
        }

        /// <inheritdoc/>
        protected override async Task CleanupDatabaseAsync()
        {
            await _Provider.CleanupDatabaseAsync();
        }

        /// <inheritdoc/>
        protected override ITransaction BeginTransaction<T>(IRepository<T> repository)
        {
            return ((MySqlRepository<T>)repository).BeginTransaction();
        }

        /// <inheritdoc/>
        protected override Task<ITransaction> BeginTransactionAsync<T>(IRepository<T> repository, CancellationToken token = default)
        {
            return ((MySqlRepository<T>)repository).BeginTransactionAsync(token);
        }

        #endregion

        #region MySQL-Specific-Tests

        /// <summary>
        /// Tests MySQL-specific SLEEP function for precise slow query simulation.
        /// </summary>
        [Fact]
        public async Task MySqlSleep_PreciseDelaySimulation()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("MySQL database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 5,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                MySqlConnectionFactory factory = new MySqlConnectionFactory(_ConnectionString, options);
                MySqlRepository<Person> repository = new MySqlRepository<Person>(factory);

                int concurrentSleeps = 10;
                double sleepSeconds = 0.5;

                System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();

                Task[] tasks = new Task[concurrentSleeps];
                for (int i = 0; i < concurrentSleeps; i++)
                {
                    tasks[i] = repository.ExecuteSqlAsync($"SELECT SLEEP({sleepSeconds})");
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                int expectedBatches = (int)Math.Ceiling((double)concurrentSleeps / options.MaxPoolSize);
                long expectedMinMs = (long)(expectedBatches * sleepSeconds * 1000);

                Console.WriteLine($"  {concurrentSleeps} × SLEEP({sleepSeconds}) with pool of {options.MaxPoolSize}");
                Console.WriteLine($"  Expected batches: {expectedBatches}, Expected min: {expectedMinMs}ms");
                Console.WriteLine($"  Actual: {stopwatch.ElapsedMilliseconds}ms");

                Assert.True(stopwatch.ElapsedMilliseconds >= expectedMinMs * 0.8,
                    $"Expected ~{expectedMinMs}ms+, got {stopwatch.ElapsedMilliseconds}ms");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests MySQL connection pool under realistic production-like load.
        /// </summary>
        [Fact]
        public async Task MySqlProductionSimulation_MixedWorkload()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("MySQL database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 5,
                    MaxPoolSize = 20,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(5)
                };

                MySqlConnectionFactory factory = new MySqlConnectionFactory(_ConnectionString, options);
                MySqlRepository<Person> repository = new MySqlRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                // Seed data
                for (int i = 0; i < 100; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Prod{i}",
                        LastName = "User",
                        Age = 20 + (i % 50),
                        Email = $"prod{i}@company.com",
                        Salary = 40000m + (i * 500),
                        Department = i % 5 == 0 ? "Engineering" : (i % 5 == 1 ? "Sales" : (i % 5 == 2 ? "Marketing" : (i % 5 == 3 ? "Support" : "HR")))
                    });
                }

                // Simulate production workload
                int fastQueryThreads = 15;
                int slowQueryThreads = 3;
                int writeThreads = 5;
                int transactionThreads = 2;
                int operationsPerThread = 30;

                int fastSuccess = 0, slowSuccess = 0, writeSuccess = 0, transSuccess = 0;
                int fastTimeout = 0, slowTimeout = 0, writeTimeout = 0, transTimeout = 0;

                Task[] allTasks = new Task[fastQueryThreads + slowQueryThreads + writeThreads + transactionThreads];
                int taskIndex = 0;

                // Fast query threads
                for (int t = 0; t < fastQueryThreads; t++)
                {
                    allTasks[taskIndex++] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread; i++)
                        {
                            try
                            {
                                Person[] results = (await repository.Query()
                                    .Where(p => p.Age > 30)
                                    .Take(10)
                                    .ExecuteAsync())
                                    .ToArray();
                                Interlocked.Increment(ref fastSuccess);
                            }
                            catch (TimeoutException)
                            {
                                Interlocked.Increment(ref fastTimeout);
                            }
                        }
                    });
                }

                // Slow query threads (using SLEEP)
                for (int t = 0; t < slowQueryThreads; t++)
                {
                    allTasks[taskIndex++] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread / 3; i++)
                        {
                            try
                            {
                                await repository.ExecuteSqlAsync("SELECT SLEEP(0.5)");
                                Interlocked.Increment(ref slowSuccess);
                            }
                            catch (TimeoutException)
                            {
                                Interlocked.Increment(ref slowTimeout);
                            }
                        }
                    });
                }

                // Write threads
                for (int t = 0; t < writeThreads; t++)
                {
                    int threadId = t;
                    allTasks[taskIndex++] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread; i++)
                        {
                            try
                            {
                                await repository.CreateAsync(new Person
                                {
                                    FirstName = $"Write{threadId}_{i}",
                                    LastName = "Test",
                                    Age = 30,
                                    Email = $"write{threadId}_{i}@test.com",
                                    Salary = 50000m,
                                    Department = "WriteTest"
                                });
                                Interlocked.Increment(ref writeSuccess);
                            }
                            catch (TimeoutException)
                            {
                                Interlocked.Increment(ref writeTimeout);
                            }
                        }
                    });
                }

                // Transaction threads
                for (int t = 0; t < transactionThreads; t++)
                {
                    int threadId = t;
                    allTasks[taskIndex++] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread / 2; i++)
                        {
                            try
                            {
                                using ITransaction transaction = await repository.BeginTransactionAsync();
                                try
                                {
                                    await repository.CreateAsync(new Person
                                    {
                                        FirstName = $"Trans{threadId}_{i}",
                                        LastName = "Test",
                                        Age = 25,
                                        Email = $"trans{threadId}_{i}@test.com",
                                        Salary = 45000m,
                                        Department = "TransTest"
                                    }, transaction);

                                    await Task.Delay(50); // Simulate some work

                                    await transaction.CommitAsync();
                                    Interlocked.Increment(ref transSuccess);
                                }
                                catch
                                {
                                    await transaction.RollbackAsync();
                                    throw;
                                }
                            }
                            catch (TimeoutException)
                            {
                                Interlocked.Increment(ref transTimeout);
                            }
                        }
                    });
                }

                await Task.WhenAll(allTasks);

                Console.WriteLine($"  Production simulation results:");
                Console.WriteLine($"    Fast queries:  {fastSuccess} success, {fastTimeout} timeout");
                Console.WriteLine($"    Slow queries:  {slowSuccess} success, {slowTimeout} timeout");
                Console.WriteLine($"    Writes:        {writeSuccess} success, {writeTimeout} timeout");
                Console.WriteLine($"    Transactions:  {transSuccess} success, {transTimeout} timeout");

                int totalSuccess = fastSuccess + slowSuccess + writeSuccess + transSuccess;
                int totalTimeout = fastTimeout + slowTimeout + writeTimeout + transTimeout;
                int total = totalSuccess + totalTimeout;

                Assert.True(totalSuccess > total * 0.9,
                    $"Expected >90% success rate in production simulation, got {totalSuccess}/{total}");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests UpsertMany without transaction (regression test for Bug 1 fix).
        /// </summary>
        [Fact]
        public async Task MySqlUpsertMany_WithoutTransaction_ShouldNotLeakConnections()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("MySQL database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 5,
                    ConnectionTimeout = TimeSpan.FromSeconds(10),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                MySqlConnectionFactory factory = new MySqlConnectionFactory(_ConnectionString, options);
                MySqlRepository<Person> repository = new MySqlRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                // Call UpsertMany 30 times without external transaction
                // Before Bug 1 fix, this would leak connections and timeout after 5 calls
                int batchCount = 30;
                int successCount = 0;
                int timeoutCount = 0;

                for (int batch = 0; batch < batchCount; batch++)
                {
                    try
                    {
                        Person[] people = new Person[3];
                        for (int i = 0; i < 3; i++)
                        {
                            people[i] = new Person
                            {
                                FirstName = $"Upsert{batch}_{i}",
                                LastName = "Test",
                                Age = 30,
                                Email = $"upsert{batch}_{i}@test.com",
                                Salary = 50000m,
                                Department = "UpsertTest"
                            };
                        }

                        System.Collections.Generic.IEnumerable<Person> result = await repository.UpsertManyAsync(people);
                        successCount++;
                    }
                    catch (TimeoutException)
                    {
                        timeoutCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"    Batch {batch} error: {ex.Message}");
                    }
                }

                Console.WriteLine($"  UpsertMany batches: {successCount} success, {timeoutCount} timeout");

                Assert.Equal(batchCount, successCount);
                Assert.Equal(0, timeoutCount);

                int count = await repository.CountAsync(p => p.Department == "UpsertTest");
                Assert.Equal(batchCount * 3, count);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests MySQL connection string with connection timeout settings.
        /// </summary>
        [Fact]
        public async Task MySqlConnectionTimeout_ShouldRespectSettings()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("MySQL database is not available");

            await SetupDatabaseAsync();

            try
            {
                // Very short timeout
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 2,
                    ConnectionTimeout = TimeSpan.FromMilliseconds(500),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                MySqlConnectionFactory factory = new MySqlConnectionFactory(_ConnectionString, options);
                MySqlRepository<Person> repository = new MySqlRepository<Person>(factory);

                // Start 5 concurrent SLEEP(2) operations with pool of 2 and timeout of 500ms
                int concurrentOps = 5;
                int timeoutCount = 0;
                int successCount = 0;

                Task[] tasks = new Task[concurrentOps];
                for (int i = 0; i < concurrentOps; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync("SELECT SLEEP(2)");
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  500ms timeout test: {successCount} success, {timeoutCount} timeout");

                // With 500ms timeout and 2s sleep, most should timeout
                Assert.True(timeoutCount > 0, "Expected timeouts with 500ms timeout and 2s queries");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region IDisposable

        /// <inheritdoc/>
        public override void Dispose()
        {
            _Provider?.Dispose();
            base.Dispose();
        }

        #endregion
    }
}
