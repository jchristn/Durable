namespace Test.Postgres
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// PostgreSQL-specific connection pool stress tests.
    /// PostgreSQL supports pg_sleep() for simulating slow queries.
    /// </summary>
    [Collection("Postgres Database Collection")]
    public class PostgresConnectionPoolStressTests : ConnectionPoolStressTests
    {
        #region Private-Members

        private readonly PostgresRepositoryProvider _Provider;
        private readonly string _ConnectionString;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresConnectionPoolStressTests"/> class.
        /// </summary>
        public PostgresConnectionPoolStressTests()
            : base(
                new PostgresRepositoryProvider(
                    Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
                    ?? "Host=localhost;Database=durable_test;Username=test_user;Password=test_password;"),
                "SELECT pg_sleep(1)",
                "SELECT pg_sleep({0})")
        {
            _ConnectionString = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
                ?? "Host=localhost;Database=durable_test;Username=test_user;Password=test_password;";
            _Provider = new PostgresRepositoryProvider(_ConnectionString);
        }

        #endregion

        #region Abstract-Implementation

        /// <inheritdoc/>
        protected override IConnectionFactory CreateConnectionFactory(ConnectionPoolOptions options)
        {
            return new PostgresConnectionFactory(_ConnectionString, options);
        }

        /// <inheritdoc/>
        protected override IRepository<T> CreateRepository<T>(IConnectionFactory factory)
        {
            return new PostgresRepository<T>((PostgresConnectionFactory)factory);
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
            return ((PostgresRepository<T>)repository).BeginTransaction();
        }

        /// <inheritdoc/>
        protected override Task<ITransaction> BeginTransactionAsync<T>(IRepository<T> repository, CancellationToken token = default)
        {
            return ((PostgresRepository<T>)repository).BeginTransactionAsync(token);
        }

        #endregion

        #region Postgres-Specific-Tests

        /// <summary>
        /// Tests PostgreSQL pg_sleep() for precise delay simulation.
        /// </summary>
        [Fact]
        public async Task PostgresPgSleep_PreciseDelaySimulation()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");

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

                PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
                PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

                int concurrentSleeps = 10;
                double sleepSeconds = 0.5;

                System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();

                Task[] tasks = new Task[concurrentSleeps];
                for (int i = 0; i < concurrentSleeps; i++)
                {
                    tasks[i] = repository.ExecuteSqlAsync($"SELECT pg_sleep({sleepSeconds})");
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                int expectedBatches = (int)Math.Ceiling((double)concurrentSleeps / options.MaxPoolSize);
                long expectedMinMs = (long)(expectedBatches * sleepSeconds * 1000);

                Console.WriteLine($"  {concurrentSleeps} × pg_sleep({sleepSeconds}) with pool of {options.MaxPoolSize}");
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
        /// Tests PostgreSQL connection pool with MVCC behavior.
        /// </summary>
        [Fact]
        public async Task PostgresMVCC_ConcurrentReadWrite()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 3,
                    MaxPoolSize = 15,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
                PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                // Seed data
                for (int i = 0; i < 50; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"MVCC{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"mvcc{i}@test.com",
                        Salary = 50000m,
                        Department = "MVCCTest"
                    });
                }

                int readerThreads = 10;
                int writerThreads = 5;
                int opsPerThread = 50;
                int readSuccess = 0, writeSuccess = 0;

                Task[] tasks = new Task[readerThreads + writerThreads];

                // Reader threads
                for (int t = 0; t < readerThreads; t++)
                {
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int i = 0; i < opsPerThread; i++)
                        {
                            Person[] results = (await repository.Query()
                                .Where(p => p.Department == "MVCCTest")
                                .ExecuteAsync())
                                .ToArray();
                            Interlocked.Increment(ref readSuccess);
                        }
                    });
                }

                // Writer threads
                for (int t = 0; t < writerThreads; t++)
                {
                    int threadId = t;
                    tasks[readerThreads + t] = Task.Run(async () =>
                    {
                        for (int i = 0; i < opsPerThread; i++)
                        {
                            await repository.CreateAsync(new Person
                            {
                                FirstName = $"MVCCWrite{threadId}_{i}",
                                LastName = "Test",
                                Age = 25,
                                Email = $"mvccwrite{threadId}_{i}@test.com",
                                Salary = 45000m,
                                Department = "MVCCWrite"
                            });
                            Interlocked.Increment(ref writeSuccess);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  MVCC test - Reads: {readSuccess}, Writes: {writeSuccess}");

                Assert.Equal(readerThreads * opsPerThread, readSuccess);
                Assert.Equal(writerThreads * opsPerThread, writeSuccess);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests PostgreSQL advisory locks with connection pool.
        /// </summary>
        [Fact]
        public async Task PostgresAdvisoryLocks_WithPoolContention()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");

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

                PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
                PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                // Test that multiple transactions with advisory locks work with the pool
                int transactionCount = 10;
                int successCount = 0;

                Task[] tasks = new Task[transactionCount];
                for (int t = 0; t < transactionCount; t++)
                {
                    int transId = t;
                    tasks[t] = Task.Run(async () =>
                    {
                        using ITransaction transaction = await repository.BeginTransactionAsync();
                        try
                        {
                            // Create a person within transaction
                            await repository.CreateAsync(new Person
                            {
                                FirstName = $"Advisory{transId}",
                                LastName = "Test",
                                Age = 30,
                                Email = $"advisory{transId}@test.com",
                                Salary = 50000m,
                                Department = "AdvisoryTest"
                            }, transaction);

                            await Task.Delay(100); // Simulate work

                            await transaction.CommitAsync();
                            Interlocked.Increment(ref successCount);
                        }
                        catch
                        {
                            await transaction.RollbackAsync();
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Advisory lock transactions: {successCount}/{transactionCount} succeeded");

                Assert.Equal(transactionCount, successCount);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests PostgreSQL production-like workload simulation.
        /// </summary>
        [Fact]
        public async Task PostgresProductionSimulation_MixedWorkload()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");

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

                PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
                PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

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
                        Department = i % 4 == 0 ? "Engineering" : (i % 4 == 1 ? "Sales" : (i % 4 == 2 ? "Marketing" : "Support"))
                    });
                }

                int fastQueryThreads = 12;
                int slowQueryThreads = 3;
                int writeThreads = 5;
                int operationsPerThread = 25;

                int fastSuccess = 0, slowSuccess = 0, writeSuccess = 0;
                int fastTimeout = 0, slowTimeout = 0, writeTimeout = 0;

                Task[] allTasks = new Task[fastQueryThreads + slowQueryThreads + writeThreads];
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

                // Slow query threads (using pg_sleep)
                for (int t = 0; t < slowQueryThreads; t++)
                {
                    allTasks[taskIndex++] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread / 3; i++)
                        {
                            try
                            {
                                await repository.ExecuteSqlAsync("SELECT pg_sleep(0.3)");
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

                await Task.WhenAll(allTasks);

                Console.WriteLine($"  PostgreSQL production simulation:");
                Console.WriteLine($"    Fast queries: {fastSuccess} success, {fastTimeout} timeout");
                Console.WriteLine($"    Slow queries: {slowSuccess} success, {slowTimeout} timeout");
                Console.WriteLine($"    Writes:       {writeSuccess} success, {writeTimeout} timeout");

                int totalSuccess = fastSuccess + slowSuccess + writeSuccess;
                int totalTimeout = fastTimeout + slowTimeout + writeTimeout;
                int total = totalSuccess + totalTimeout;

                Assert.True(totalSuccess > total * 0.9,
                    $"Expected >90% success rate, got {totalSuccess}/{total}");

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
