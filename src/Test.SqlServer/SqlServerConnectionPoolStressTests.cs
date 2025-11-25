namespace Test.SqlServer
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.SqlServer;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// SQL Server-specific connection pool stress tests.
    /// SQL Server supports WAITFOR DELAY for simulating slow queries.
    /// </summary>
    [Collection("SqlServer Database Collection")]
    public class SqlServerConnectionPoolStressTests : ConnectionPoolStressTests
    {
        #region Private-Members

        private readonly SqlServerRepositoryProvider _Provider;
        private readonly string _ConnectionString;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerConnectionPoolStressTests"/> class.
        /// </summary>
        public SqlServerConnectionPoolStressTests()
            : base(
                new SqlServerRepositoryProvider(
                    Environment.GetEnvironmentVariable("SQLSERVER_CONNECTION_STRING")
                    ?? "Server=localhost;Database=durable_test;User Id=test_user;Password=test_password;TrustServerCertificate=True;"),
                "WAITFOR DELAY '00:00:01'",
                "WAITFOR DELAY '00:00:0{0}'")  // SQL Server format requires specific time format
        {
            _ConnectionString = Environment.GetEnvironmentVariable("SQLSERVER_CONNECTION_STRING")
                ?? "Server=localhost;Database=durable_test;User Id=test_user;Password=test_password;TrustServerCertificate=True;";
            _Provider = new SqlServerRepositoryProvider(_ConnectionString);
        }

        #endregion

        #region Abstract-Implementation

        /// <inheritdoc/>
        protected override IConnectionFactory CreateConnectionFactory(ConnectionPoolOptions options)
        {
            return new SqlServerConnectionFactory(_ConnectionString, options);
        }

        /// <inheritdoc/>
        protected override IRepository<T> CreateRepository<T>(IConnectionFactory factory)
        {
            return new SqlServerRepository<T>((SqlServerConnectionFactory)factory);
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
            return ((SqlServerRepository<T>)repository).BeginTransaction();
        }

        /// <inheritdoc/>
        protected override Task<ITransaction> BeginTransactionAsync<T>(IRepository<T> repository, CancellationToken token = default)
        {
            return ((SqlServerRepository<T>)repository).BeginTransactionAsync(token);
        }

        #endregion

        #region SqlServer-Specific-Tests

        /// <summary>
        /// Tests SQL Server WAITFOR DELAY for precise delay simulation.
        /// </summary>
        [Fact]
        public async Task SqlServerWaitFor_PreciseDelaySimulation()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("SQL Server database is not available");

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

                SqlServerConnectionFactory factory = new SqlServerConnectionFactory(_ConnectionString, options);
                SqlServerRepository<Person> repository = new SqlServerRepository<Person>(factory);

                int concurrentWaits = 10;
                int waitMilliseconds = 500;

                System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();

                Task[] tasks = new Task[concurrentWaits];
                for (int i = 0; i < concurrentWaits; i++)
                {
                    tasks[i] = repository.ExecuteSqlAsync($"WAITFOR DELAY '00:00:00.{waitMilliseconds}'");
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                int expectedBatches = (int)Math.Ceiling((double)concurrentWaits / options.MaxPoolSize);
                long expectedMinMs = expectedBatches * waitMilliseconds;

                Console.WriteLine($"  {concurrentWaits} × WAITFOR DELAY {waitMilliseconds}ms with pool of {options.MaxPoolSize}");
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
        /// Tests SQL Server snapshot isolation with connection pool.
        /// </summary>
        [Fact]
        public async Task SqlServerConcurrentReadWrite_WithPoolContention()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("SQL Server database is not available");

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

                SqlServerConnectionFactory factory = new SqlServerConnectionFactory(_ConnectionString, options);
                SqlServerRepository<Person> repository = new SqlServerRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                // Seed data
                for (int i = 0; i < 50; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Concurrent{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"concurrent{i}@test.com",
                        Salary = 50000m,
                        Department = "ConcurrentTest"
                    });
                }

                int readerThreads = 10;
                int writerThreads = 5;
                int opsPerThread = 50;
                int readSuccess = 0, writeSuccess = 0;
                int readError = 0, writeError = 0;

                Task[] tasks = new Task[readerThreads + writerThreads];

                // Reader threads
                for (int t = 0; t < readerThreads; t++)
                {
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int i = 0; i < opsPerThread; i++)
                        {
                            try
                            {
                                Person[] results = (await repository.Query()
                                    .Where(p => p.Department == "ConcurrentTest")
                                    .ExecuteAsync())
                                    .ToArray();
                                Interlocked.Increment(ref readSuccess);
                            }
                            catch
                            {
                                Interlocked.Increment(ref readError);
                            }
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
                            try
                            {
                                await repository.CreateAsync(new Person
                                {
                                    FirstName = $"Writer{threadId}_{i}",
                                    LastName = "Test",
                                    Age = 25,
                                    Email = $"writer{threadId}_{i}@test.com",
                                    Salary = 45000m,
                                    Department = "WriterTest"
                                });
                                Interlocked.Increment(ref writeSuccess);
                            }
                            catch
                            {
                                Interlocked.Increment(ref writeError);
                            }
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Concurrent R/W - Reads: {readSuccess} (err: {readError}), Writes: {writeSuccess} (err: {writeError})");

                Assert.True(readSuccess > readerThreads * opsPerThread * 0.9,
                    $"Expected >90% read success rate");
                Assert.True(writeSuccess > writerThreads * opsPerThread * 0.9,
                    $"Expected >90% write success rate");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests SQL Server production-like workload simulation.
        /// </summary>
        [Fact]
        public async Task SqlServerProductionSimulation_MixedWorkload()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("SQL Server database is not available");

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

                SqlServerConnectionFactory factory = new SqlServerConnectionFactory(_ConnectionString, options);
                SqlServerRepository<Person> repository = new SqlServerRepository<Person>(factory);

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
                int transactionThreads = 3;
                int operationsPerThread = 25;

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

                // Slow query threads (using WAITFOR)
                for (int t = 0; t < slowQueryThreads; t++)
                {
                    allTasks[taskIndex++] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread / 3; i++)
                        {
                            try
                            {
                                await repository.ExecuteSqlAsync("WAITFOR DELAY '00:00:00.300'");
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
                                        Age = 28,
                                        Email = $"trans{threadId}_{i}@test.com",
                                        Salary = 48000m,
                                        Department = "TransTest"
                                    }, transaction);

                                    await Task.Delay(30);

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

                Console.WriteLine($"  SQL Server production simulation:");
                Console.WriteLine($"    Fast queries:  {fastSuccess} success, {fastTimeout} timeout");
                Console.WriteLine($"    Slow queries:  {slowSuccess} success, {slowTimeout} timeout");
                Console.WriteLine($"    Writes:        {writeSuccess} success, {writeTimeout} timeout");
                Console.WriteLine($"    Transactions:  {transSuccess} success, {transTimeout} timeout");

                int totalSuccess = fastSuccess + slowSuccess + writeSuccess + transSuccess;
                int totalTimeout = fastTimeout + slowTimeout + writeTimeout + transTimeout;
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

        /// <summary>
        /// Tests SQL Server deadlock handling with connection pool.
        /// </summary>
        [Fact]
        public async Task SqlServerTransactionIsolation_ConcurrentTransactions()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("SQL Server database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 10,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                SqlServerConnectionFactory factory = new SqlServerConnectionFactory(_ConnectionString, options);
                SqlServerRepository<Person> repository = new SqlServerRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                // Seed data
                for (int i = 0; i < 20; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Isolation{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"isolation{i}@test.com",
                        Salary = 50000m,
                        Department = "IsolationTest"
                    });
                }

                int transactionCount = 15;
                int successCount = 0;
                int errorCount = 0;

                Task[] tasks = new Task[transactionCount];
                for (int t = 0; t < transactionCount; t++)
                {
                    int transId = t;
                    tasks[t] = Task.Run(async () =>
                    {
                        try
                        {
                            using ITransaction transaction = await repository.BeginTransactionAsync();
                            try
                            {
                                // Read and update within transaction
                                Person? person = await repository.ReadFirstAsync(
                                    p => p.Department == "IsolationTest", transaction);

                                if (person != null)
                                {
                                    person.Age = person.Age + 1;
                                    await repository.UpdateAsync(person, transaction);
                                }

                                await Task.Delay(50);

                                await transaction.CommitAsync();
                                Interlocked.Increment(ref successCount);
                            }
                            catch
                            {
                                await transaction.RollbackAsync();
                                throw;
                            }
                        }
                        catch
                        {
                            Interlocked.Increment(ref errorCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Transaction isolation test: {successCount} success, {errorCount} errors");

                // Some transactions may fail due to contention, but most should succeed
                Assert.True(successCount > transactionCount * 0.5,
                    $"Expected >50% transaction success rate, got {successCount}/{transactionCount}");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests very short connection timeout behavior.
        /// </summary>
        [Fact]
        public async Task SqlServerShortTimeout_ShouldFailFast()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("SQL Server database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 2,
                    ConnectionTimeout = TimeSpan.FromMilliseconds(500),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                SqlServerConnectionFactory factory = new SqlServerConnectionFactory(_ConnectionString, options);
                SqlServerRepository<Person> repository = new SqlServerRepository<Person>(factory);

                // Start concurrent WAITFOR operations that will exhaust the tiny pool
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
                            await repository.ExecuteSqlAsync("WAITFOR DELAY '00:00:02'");
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Short timeout test (500ms): {successCount} success, {timeoutCount} timeout");

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
