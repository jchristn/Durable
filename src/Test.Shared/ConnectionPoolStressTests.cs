namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Comprehensive stress test suite for connection pool management.
    /// Tests pool exhaustion, slow queries, large result sets, transaction contention,
    /// exception recovery, and various load levels to surface timeout issues.
    /// </summary>
    public abstract class ConnectionPoolStressTests : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;
        private readonly string _SleepCommand;
        private readonly string _SleepCommandFormat;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionPoolStressTests"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        /// <param name="sleepCommand">The SQL command to sleep for 1 second (e.g., "SELECT SLEEP(1)" for MySQL).</param>
        /// <param name="sleepCommandFormat">Format string for sleep command with placeholder for seconds (e.g., "SELECT SLEEP({0})").</param>
        protected ConnectionPoolStressTests(
            IRepositoryProvider provider,
            string sleepCommand,
            string sleepCommandFormat)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _SleepCommand = sleepCommand ?? throw new ArgumentNullException(nameof(sleepCommand));
            _SleepCommandFormat = sleepCommandFormat ?? throw new ArgumentNullException(nameof(sleepCommandFormat));
        }

        #endregion

        #region Abstract-Methods

        /// <summary>
        /// Creates a connection factory with the specified pool options.
        /// </summary>
        /// <param name="options">The connection pool options.</param>
        /// <returns>A connection factory configured with the specified options.</returns>
        protected abstract IConnectionFactory CreateConnectionFactory(ConnectionPoolOptions options);

        /// <summary>
        /// Creates a repository using the specified connection factory.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="factory">The connection factory.</param>
        /// <returns>A repository for the entity type.</returns>
        protected abstract IRepository<T> CreateRepository<T>(IConnectionFactory factory) where T : class, new();

        /// <summary>
        /// Checks if the database is available for testing.
        /// </summary>
        /// <returns>True if the database is available, false otherwise.</returns>
        protected abstract Task<bool> IsDatabaseAvailableAsync();

        /// <summary>
        /// Sets up the database schema for testing.
        /// </summary>
        protected abstract Task SetupDatabaseAsync();

        /// <summary>
        /// Cleans up the database after testing.
        /// </summary>
        protected abstract Task CleanupDatabaseAsync();

        /// <summary>
        /// Begins a transaction on the repository.
        /// </summary>
        /// <param name="repository">The repository.</param>
        /// <returns>A transaction.</returns>
        protected abstract ITransaction BeginTransaction<T>(IRepository<T> repository) where T : class, new();

        /// <summary>
        /// Begins a transaction asynchronously on the repository.
        /// </summary>
        /// <param name="repository">The repository.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A transaction.</returns>
        protected abstract Task<ITransaction> BeginTransactionAsync<T>(IRepository<T> repository, CancellationToken token = default) where T : class, new();

        #endregion

        #region Category-1-Pool-Exhaustion-Tests

        /// <summary>
        /// Tests that TimeoutException is thrown when pool is exhausted with tiny pool.
        /// </summary>
        [Fact]
        public virtual async Task PoolExhaustion_TinyPool_ShouldThrowTimeoutException()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 2,
                    ConnectionTimeout = TimeSpan.FromSeconds(2),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                int concurrentOperations = 10;
                int successCount = 0;
                int timeoutCount = 0;
                List<Exception> otherExceptions = new List<Exception>();

                Task[] tasks = new Task[concurrentOperations];
                for (int i = 0; i < concurrentOperations; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync(string.Format(_SleepCommandFormat, 3));
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                        catch (Exception ex)
                        {
                            lock (otherExceptions)
                            {
                                otherExceptions.Add(ex);
                            }
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Pool size: {options.MaxPoolSize}, Concurrent ops: {concurrentOperations}");
                Console.WriteLine($"  Success: {successCount}, Timeout: {timeoutCount}, Other errors: {otherExceptions.Count}");

                Assert.True(timeoutCount > 0, "Expected at least some TimeoutExceptions with tiny pool");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests pool exhaustion with small pool under moderate load.
        /// </summary>
        [Fact]
        public virtual async Task PoolExhaustion_SmallPool_ModerateLoad()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 5,
                    ConnectionTimeout = TimeSpan.FromSeconds(3),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                int concurrentOperations = 15;
                int successCount = 0;
                int timeoutCount = 0;

                Task[] tasks = new Task[concurrentOperations];
                for (int i = 0; i < concurrentOperations; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync(string.Format(_SleepCommandFormat, 2));
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Pool size: {options.MaxPoolSize}, Concurrent ops: {concurrentOperations}");
                Console.WriteLine($"  Success: {successCount}, Timeout: {timeoutCount}");

                Assert.True(timeoutCount > 0, "Expected some TimeoutExceptions with small pool under load");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests that pool handles exactly max capacity without timeouts.
        /// </summary>
        [Fact]
        public virtual async Task PoolAtCapacity_ShouldSucceedWithoutTimeout()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

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

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                int concurrentOperations = options.MaxPoolSize;
                int successCount = 0;
                int timeoutCount = 0;

                Task[] tasks = new Task[concurrentOperations];
                for (int i = 0; i < concurrentOperations; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync(string.Format(_SleepCommandFormat, 1));
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Pool size: {options.MaxPoolSize}, Concurrent ops: {concurrentOperations}");
                Console.WriteLine($"  Success: {successCount}, Timeout: {timeoutCount}");

                Assert.Equal(concurrentOperations, successCount);
                Assert.Equal(0, timeoutCount);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-2-Slow-Query-Tests

        /// <summary>
        /// Tests connection pool behavior with slow queries causing backpressure.
        /// </summary>
        [Fact]
        public virtual async Task SlowQueries_ShouldCreateBackpressure()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

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

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                int concurrentSlowQueries = 10;
                double queryDurationSeconds = 0.5;

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[concurrentSlowQueries];
                for (int i = 0; i < concurrentSlowQueries; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        await repository.ExecuteSqlAsync(string.Format(_SleepCommandFormat, queryDurationSeconds));
                    });
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                int expectedBatches = (int)Math.Ceiling((double)concurrentSlowQueries / options.MaxPoolSize);
                long expectedMinTimeMs = (long)(expectedBatches * queryDurationSeconds * 1000);

                Console.WriteLine($"  {concurrentSlowQueries} queries × {queryDurationSeconds}s each, pool of {options.MaxPoolSize}");
                Console.WriteLine($"  Expected batches: {expectedBatches}, Expected min time: {expectedMinTimeMs}ms");
                Console.WriteLine($"  Actual time: {stopwatch.ElapsedMilliseconds}ms");

                Assert.True(stopwatch.ElapsedMilliseconds >= expectedMinTimeMs * 0.8,
                    $"Expected ~{expectedMinTimeMs}ms+ due to pool contention, got {stopwatch.ElapsedMilliseconds}ms");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests varying slow query durations.
        /// </summary>
        [Fact]
        public virtual async Task SlowQueries_VaryingDurations_ShouldComplete()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 8,
                    ConnectionTimeout = TimeSpan.FromSeconds(60),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                double[] durations = { 0.1, 0.2, 0.5, 1.0, 0.3, 0.7, 0.4, 0.8, 0.2, 0.6 };
                int successCount = 0;

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[durations.Length];
                for (int i = 0; i < durations.Length; i++)
                {
                    double duration = durations[i];
                    tasks[i] = Task.Run(async () =>
                    {
                        await repository.ExecuteSqlAsync(string.Format(_SleepCommandFormat, duration));
                        Interlocked.Increment(ref successCount);
                    });
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                Console.WriteLine($"  {durations.Length} queries with varying durations: {stopwatch.ElapsedMilliseconds}ms");
                Assert.Equal(durations.Length, successCount);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-3-Large-Result-Set-Tests

        /// <summary>
        /// Tests connection holding during large result set reading.
        /// </summary>
        [Fact]
        public virtual async Task LargeResultSet_ShouldHoldConnectionDuringRead()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 5,
                    ConnectionTimeout = TimeSpan.FromSeconds(60),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int recordCount = 5000;
                int batchSize = 100;

                Console.WriteLine($"  Inserting {recordCount} records...");

                for (int batch = 0; batch < recordCount / batchSize; batch++)
                {
                    Person[] people = new Person[batchSize];
                    for (int i = 0; i < batchSize; i++)
                    {
                        int idx = batch * batchSize + i;
                        people[i] = new Person
                        {
                            FirstName = $"Large{idx}",
                            LastName = $"Result{idx}",
                            Age = 25 + (idx % 50),
                            Email = $"large{idx}@test.com",
                            Salary = 50000m,
                            Department = "LargeTest"
                        };
                    }
                    await repository.CreateManyAsync(people);
                }

                Console.WriteLine($"  Reading {recordCount} records with {options.MaxPoolSize} concurrent readers...");

                int concurrentReaders = options.MaxPoolSize;
                Stopwatch stopwatch = Stopwatch.StartNew();

                Task<int>[] tasks = new Task<int>[concurrentReaders];
                for (int i = 0; i < concurrentReaders; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        Person[] results = (await repository.Query()
                            .Where(p => p.Department == "LargeTest")
                            .ExecuteAsync())
                            .ToArray();
                        return results.Length;
                    });
                }

                int[] counts = await Task.WhenAll(tasks);
                stopwatch.Stop();

                foreach (int count in counts)
                {
                    Assert.Equal(recordCount, count);
                }

                Console.WriteLine($"  {concurrentReaders} concurrent reads of {recordCount} records: {stopwatch.ElapsedMilliseconds}ms");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests large result sets with pool smaller than reader count.
        /// </summary>
        [Fact]
        public virtual async Task LargeResultSet_PoolSmallerThanReaders_ShouldQueue()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 3,
                    ConnectionTimeout = TimeSpan.FromSeconds(120),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int recordCount = 2000;
                int batchSize = 100;

                for (int batch = 0; batch < recordCount / batchSize; batch++)
                {
                    Person[] people = new Person[batchSize];
                    for (int i = 0; i < batchSize; i++)
                    {
                        int idx = batch * batchSize + i;
                        people[i] = new Person
                        {
                            FirstName = $"Queue{idx}",
                            LastName = $"Test{idx}",
                            Age = 30,
                            Email = $"queue{idx}@test.com",
                            Salary = 50000m,
                            Department = "QueueTest"
                        };
                    }
                    await repository.CreateManyAsync(people);
                }

                int concurrentReaders = 10;
                int successCount = 0;
                int timeoutCount = 0;

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[concurrentReaders];
                for (int i = 0; i < concurrentReaders; i++)
                {
                    tasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            Person[] results = (await repository.Query()
                                .Where(p => p.Department == "QueueTest")
                                .ExecuteAsync())
                                .ToArray();
                            Assert.Equal(recordCount, results.Length);
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                Console.WriteLine($"  Pool: {options.MaxPoolSize}, Readers: {concurrentReaders}, Records: {recordCount}");
                Console.WriteLine($"  Success: {successCount}, Timeout: {timeoutCount}, Time: {stopwatch.ElapsedMilliseconds}ms");

                Assert.Equal(concurrentReaders, successCount);
                Assert.Equal(0, timeoutCount);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-4-Transaction-Contention-Tests

        /// <summary>
        /// Tests pool behavior with long-running transactions competing with regular queries.
        /// </summary>
        [Fact]
        public virtual async Task TransactionContention_LongTransactions_ShouldNotStarveQueries()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 8,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                for (int i = 0; i < 20; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Trans{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"trans{i}@test.com",
                        Salary = 50000m,
                        Department = "TransTest"
                    });
                }

                int longTransactionCount = 4;
                int transactionHoldTimeMs = 3000;

                Task[] transactionTasks = new Task[longTransactionCount];
                for (int t = 0; t < longTransactionCount; t++)
                {
                    int transId = t;
                    transactionTasks[t] = Task.Run(async () =>
                    {
                        using ITransaction transaction = await BeginTransactionAsync(repository);
                        try
                        {
                            int count = await repository.CountAsync(p => p.Department == "TransTest", transaction);
                            await Task.Delay(transactionHoldTimeMs);
                            await transaction.CommitAsync();
                        }
                        catch
                        {
                            await transaction.RollbackAsync();
                            throw;
                        }
                    });
                }

                await Task.Delay(200);

                int regularQueryCount = 20;
                int successfulQueries = 0;
                int timedOutQueries = 0;

                Task[] queryTasks = new Task[regularQueryCount];
                for (int q = 0; q < regularQueryCount; q++)
                {
                    queryTasks[q] = Task.Run(async () =>
                    {
                        try
                        {
                            int count = await repository.CountAsync();
                            Interlocked.Increment(ref successfulQueries);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timedOutQueries);
                        }
                    });
                }

                await Task.WhenAll(queryTasks);
                await Task.WhenAll(transactionTasks);

                Console.WriteLine($"  Pool: {options.MaxPoolSize}, Long transactions: {longTransactionCount} ({transactionHoldTimeMs}ms each)");
                Console.WriteLine($"  Regular queries - Success: {successfulQueries}, Timeout: {timedOutQueries}");

                Assert.True(successfulQueries > 0, "Some queries should succeed");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests multiple concurrent transactions.
        /// </summary>
        [Fact]
        public virtual async Task ConcurrentTransactions_ShouldAllComplete()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

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

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int transactionCount = 20;
                int successCount = 0;
                int failCount = 0;

                Task[] tasks = new Task[transactionCount];
                for (int t = 0; t < transactionCount; t++)
                {
                    int transId = t;
                    tasks[t] = Task.Run(async () =>
                    {
                        try
                        {
                            using ITransaction transaction = await BeginTransactionAsync(repository);
                            try
                            {
                                await repository.CreateAsync(new Person
                                {
                                    FirstName = $"ConcTrans{transId}",
                                    LastName = "Test",
                                    Age = 25,
                                    Email = $"conctrans{transId}@test.com",
                                    Salary = 50000m,
                                    Department = "ConcTransTest"
                                }, transaction);

                                await Task.Delay(100);

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
                            Interlocked.Increment(ref failCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  {transactionCount} concurrent transactions - Success: {successCount}, Failed: {failCount}");

                int count = await repository.CountAsync(p => p.Department == "ConcTransTest");
                Assert.Equal(successCount, count);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-5-Exception-Recovery-Tests

        /// <summary>
        /// Tests that connections are properly returned after SQL exceptions.
        /// </summary>
        [Fact]
        public virtual async Task ExceptionDuringQuery_ShouldReturnConnectionToPool()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 3,
                    ConnectionTimeout = TimeSpan.FromSeconds(5),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int exceptionCount = 50;
                for (int i = 0; i < exceptionCount; i++)
                {
                    try
                    {
                        await repository.ExecuteSqlAsync("SELECT * FROM nonexistent_table_xyz_12345");
                    }
                    catch
                    {
                        // Expected - invalid table
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    Person person = new Person
                    {
                        FirstName = "Recovery",
                        LastName = "Test",
                        Age = 30,
                        Email = $"recovery{i}@test.com",
                        Salary = 50000m,
                        Department = "Recovery"
                    };

                    await repository.CreateAsync(person);
                }

                int count = await repository.CountAsync(p => p.Department == "Recovery");
                Assert.Equal(10, count);

                Console.WriteLine($"  {exceptionCount} exceptions followed by 10 successful operations");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests concurrent exceptions don't corrupt pool state.
        /// </summary>
        [Fact]
        public virtual async Task ConcurrentExceptions_ShouldNotCorruptPool()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 5,
                    ConnectionTimeout = TimeSpan.FromSeconds(10),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                int concurrentExceptions = 20;
                Task[] exceptionTasks = new Task[concurrentExceptions];

                for (int i = 0; i < concurrentExceptions; i++)
                {
                    exceptionTasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync("INVALID SQL SYNTAX HERE!!!");
                        }
                        catch
                        {
                            // Expected
                        }
                    });
                }

                await Task.WhenAll(exceptionTasks);

                int successCount = 0;
                int timeoutCount = 0;
                Task[] recoveryTasks = new Task[10];

                for (int i = 0; i < 10; i++)
                {
                    recoveryTasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync("SELECT 1");
                            Interlocked.Increment(ref successCount);
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(recoveryTasks);

                Console.WriteLine($"  After {concurrentExceptions} concurrent exceptions:");
                Console.WriteLine($"  Recovery queries - Success: {successCount}, Timeout: {timeoutCount}");

                Assert.Equal(10, successCount);
                Assert.Equal(0, timeoutCount);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests transaction rollback properly returns connection.
        /// </summary>
        [Fact]
        public virtual async Task TransactionRollback_ShouldReturnConnection()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 3,
                    ConnectionTimeout = TimeSpan.FromSeconds(5),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int rollbackCount = 20;
                for (int i = 0; i < rollbackCount; i++)
                {
                    using ITransaction transaction = await BeginTransactionAsync(repository);
                    try
                    {
                        await repository.CreateAsync(new Person
                        {
                            FirstName = $"Rollback{i}",
                            LastName = "Test",
                            Age = 30,
                            Email = $"rollback{i}@test.com",
                            Salary = 50000m,
                            Department = "RollbackTest"
                        }, transaction);

                        await transaction.RollbackAsync();
                    }
                    catch
                    {
                        await transaction.RollbackAsync();
                    }
                }

                int count = await repository.CountAsync();
                Assert.Equal(0, count);

                await repository.CreateAsync(new Person
                {
                    FirstName = "After",
                    LastName = "Rollbacks",
                    Age = 30,
                    Email = "after@test.com",
                    Salary = 50000m,
                    Department = "AfterTest"
                });

                count = await repository.CountAsync();
                Assert.Equal(1, count);

                Console.WriteLine($"  {rollbackCount} rollbacks completed, pool still functional");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-6-Load-Level-Tests

        /// <summary>
        /// Tests low load scenario.
        /// </summary>
        [Fact]
        public virtual async Task LowLoad_Sequential_ShouldComplete()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 2,
                    MaxPoolSize = 10,
                    ConnectionTimeout = TimeSpan.FromSeconds(10),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                Stopwatch stopwatch = Stopwatch.StartNew();

                for (int i = 0; i < 100; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"LowLoad{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"lowload{i}@test.com",
                        Salary = 50000m,
                        Department = "LowLoad"
                    });
                }

                stopwatch.Stop();

                int count = await repository.CountAsync(p => p.Department == "LowLoad");
                Assert.Equal(100, count);

                Console.WriteLine($"  100 sequential inserts: {stopwatch.ElapsedMilliseconds}ms");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests medium load scenario.
        /// </summary>
        [Fact]
        public virtual async Task MediumLoad_Concurrent_ShouldComplete()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

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

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                for (int i = 0; i < 100; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Medium{i}",
                        LastName = "Test",
                        Age = 25 + (i % 40),
                        Email = $"medium{i}@test.com",
                        Salary = 50000m,
                        Department = "MediumLoad"
                    });
                }

                int threadCount = 10;
                int queriesPerThread = 100;

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int q = 0; q < queriesPerThread; q++)
                        {
                            Person[] results = (await repository.Query()
                                .Where(p => p.Age > 30)
                                .Take(10)
                                .ExecuteAsync())
                                .ToArray();
                            Assert.NotNull(results);
                        }
                    });
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                Console.WriteLine($"  {threadCount} threads × {queriesPerThread} queries = {threadCount * queriesPerThread} total");
                Console.WriteLine($"  Time: {stopwatch.ElapsedMilliseconds}ms, Avg: {stopwatch.ElapsedMilliseconds / (double)(threadCount * queriesPerThread):F2}ms/query");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests high load scenario.
        /// </summary>
        [Fact]
        public virtual async Task HighLoad_Concurrent_ShouldComplete()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 5,
                    MaxPoolSize = 25,
                    ConnectionTimeout = TimeSpan.FromSeconds(60),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                for (int i = 0; i < 200; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"High{i}",
                        LastName = "Test",
                        Age = 25 + (i % 40),
                        Email = $"high{i}@test.com",
                        Salary = 50000m,
                        Department = "HighLoad"
                    });
                }

                int threadCount = 25;
                int queriesPerThread = 200;

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int q = 0; q < queriesPerThread; q++)
                        {
                            Person[] results = (await repository.Query()
                                .Where(p => p.Age > 30)
                                .Take(20)
                                .ExecuteAsync())
                                .ToArray();
                            Assert.NotNull(results);
                        }
                    });
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                Console.WriteLine($"  {threadCount} threads × {queriesPerThread} queries = {threadCount * queriesPerThread} total");
                Console.WriteLine($"  Time: {stopwatch.ElapsedMilliseconds}ms, Avg: {stopwatch.ElapsedMilliseconds / (double)(threadCount * queriesPerThread):F2}ms/query");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests extreme load scenario.
        /// </summary>
        [Fact]
        public virtual async Task ExtremeLoad_ShouldHandleGracefully()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 5,
                    MaxPoolSize = 50,
                    ConnectionTimeout = TimeSpan.FromSeconds(120),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                for (int i = 0; i < 500; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Extreme{i}",
                        LastName = "Test",
                        Age = 25 + (i % 40),
                        Email = $"extreme{i}@test.com",
                        Salary = 50000m,
                        Department = "ExtremeLoad"
                    });
                }

                int threadCount = 50;
                int queriesPerThread = 100;
                int successCount = 0;
                int timeoutCount = 0;

                Stopwatch stopwatch = Stopwatch.StartNew();

                Task[] tasks = new Task[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int q = 0; q < queriesPerThread; q++)
                        {
                            try
                            {
                                Person[] results = (await repository.Query()
                                    .Where(p => p.Age > 30)
                                    .Take(20)
                                    .ExecuteAsync())
                                    .ToArray();
                                Interlocked.Increment(ref successCount);
                            }
                            catch (TimeoutException)
                            {
                                Interlocked.Increment(ref timeoutCount);
                            }
                        }
                    });
                }

                await Task.WhenAll(tasks);
                stopwatch.Stop();

                int totalQueries = threadCount * queriesPerThread;
                Console.WriteLine($"  {threadCount} threads × {queriesPerThread} queries = {totalQueries} total");
                Console.WriteLine($"  Success: {successCount}, Timeout: {timeoutCount}");
                Console.WriteLine($"  Time: {stopwatch.ElapsedMilliseconds}ms");

                Assert.True(successCount > totalQueries * 0.95,
                    $"Expected >95% success rate, got {successCount}/{totalQueries}");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-7-Mixed-Operations-Tests

        /// <summary>
        /// Tests mixed read/write operations under load.
        /// </summary>
        [Fact]
        public virtual async Task MixedReadWrite_UnderLoad_ShouldComplete()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

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

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int threadCount = 10;
                int operationsPerThread = 50;
                int createCount = 0;
                int readCount = 0;
                int updateCount = 0;

                Task[] tasks = new Task[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    int threadId = t;
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int op = 0; op < operationsPerThread; op++)
                        {
                            int operation = (threadId * operationsPerThread + op) % 4;

                            switch (operation)
                            {
                                case 0:
                                    await repository.CreateAsync(new Person
                                    {
                                        FirstName = $"Mixed{threadId}_{op}",
                                        LastName = "Test",
                                        Age = 30,
                                        Email = $"mixed{threadId}_{op}@test.com",
                                        Salary = 50000m,
                                        Department = "MixedOps"
                                    });
                                    Interlocked.Increment(ref createCount);
                                    break;

                                case 1:
                                    Person[] people = (await repository.Query()
                                        .Where(p => p.Department == "MixedOps")
                                        .Take(5)
                                        .ExecuteAsync())
                                        .ToArray();
                                    Interlocked.Increment(ref readCount);
                                    break;

                                case 2:
                                    Person? personToUpdate = await repository.ReadFirstAsync(
                                        p => p.Department == "MixedOps");
                                    if (personToUpdate != null)
                                    {
                                        personToUpdate.Age += 1;
                                        await repository.UpdateAsync(personToUpdate);
                                        Interlocked.Increment(ref updateCount);
                                    }
                                    break;

                                case 3:
                                    int count = await repository.CountAsync(p => p.Department == "MixedOps");
                                    Interlocked.Increment(ref readCount);
                                    break;
                            }
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Creates: {createCount}, Reads: {readCount}, Updates: {updateCount}");
                Assert.True(createCount > 0 && readCount > 0);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests transactions mixed with regular operations.
        /// </summary>
        [Fact]
        public virtual async Task TransactionsWithRegularOps_ShouldCoexist()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 3,
                    MaxPoolSize = 10,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int transactionThreads = 5;
                int regularThreads = 10;
                int opsPerThread = 20;

                Task[] allTasks = new Task[transactionThreads + regularThreads];

                for (int t = 0; t < transactionThreads; t++)
                {
                    int threadId = t;
                    allTasks[t] = Task.Run(async () =>
                    {
                        for (int op = 0; op < opsPerThread; op++)
                        {
                            using ITransaction transaction = await BeginTransactionAsync(repository);
                            try
                            {
                                await repository.CreateAsync(new Person
                                {
                                    FirstName = $"Trans{threadId}_{op}",
                                    LastName = "Test",
                                    Age = 30,
                                    Email = $"trans{threadId}_{op}@test.com",
                                    Salary = 50000m,
                                    Department = "TransOps"
                                }, transaction);
                                await transaction.CommitAsync();
                            }
                            catch
                            {
                                await transaction.RollbackAsync();
                            }
                        }
                    });
                }

                for (int t = 0; t < regularThreads; t++)
                {
                    int threadId = t;
                    allTasks[transactionThreads + t] = Task.Run(async () =>
                    {
                        for (int op = 0; op < opsPerThread; op++)
                        {
                            await repository.CreateAsync(new Person
                            {
                                FirstName = $"Regular{threadId}_{op}",
                                LastName = "Test",
                                Age = 25,
                                Email = $"regular{threadId}_{op}@test.com",
                                Salary = 45000m,
                                Department = "RegularOps"
                            });
                        }
                    });
                }

                await Task.WhenAll(allTasks);

                int transCount = await repository.CountAsync(p => p.Department == "TransOps");
                int regularCount = await repository.CountAsync(p => p.Department == "RegularOps");

                Console.WriteLine($"  Transaction ops: {transCount}, Regular ops: {regularCount}");
                Assert.True(transCount > 0 && regularCount > 0);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region Category-8-Pool-Recovery-Tests

        /// <summary>
        /// Tests pool recovery after exhaustion.
        /// </summary>
        [Fact]
        public virtual async Task PoolRecovery_AfterExhaustion_ShouldResume()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 3,
                    ConnectionTimeout = TimeSpan.FromSeconds(2),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int exhaustionAttempts = 10;
                int timeoutsDuringExhaustion = 0;

                Task[] exhaustionTasks = new Task[exhaustionAttempts];
                for (int i = 0; i < exhaustionAttempts; i++)
                {
                    exhaustionTasks[i] = Task.Run(async () =>
                    {
                        try
                        {
                            await repository.ExecuteSqlAsync(string.Format(_SleepCommandFormat, 3));
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutsDuringExhaustion);
                        }
                    });
                }

                await Task.WhenAll(exhaustionTasks);

                Console.WriteLine($"  Exhaustion phase: {timeoutsDuringExhaustion} timeouts");

                await Task.Delay(1000);

                int recoverySuccessCount = 0;
                int recoveryTimeoutCount = 0;

                for (int i = 0; i < 10; i++)
                {
                    try
                    {
                        await repository.CreateAsync(new Person
                        {
                            FirstName = $"Recovery{i}",
                            LastName = "Test",
                            Age = 30,
                            Email = $"recovery{i}@test.com",
                            Salary = 50000m,
                            Department = "Recovery"
                        });
                        recoverySuccessCount++;
                    }
                    catch (TimeoutException)
                    {
                        recoveryTimeoutCount++;
                    }
                }

                Console.WriteLine($"  Recovery phase: {recoverySuccessCount} successes, {recoveryTimeoutCount} timeouts");

                Assert.True(recoverySuccessCount >= 8,
                    $"Expected pool to recover, got {recoverySuccessCount}/10 successes");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests sustained load doesn't leak connections over time.
        /// </summary>
        [Fact]
        public virtual async Task SustainedLoad_ShouldNotLeak()
        {
            if (!await IsDatabaseAvailableAsync())
                throw new SharedTestRunner.SkipException("Database is not available");

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

                IConnectionFactory factory = CreateConnectionFactory(options);
                IRepository<Person> repository = CreateRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                for (int i = 0; i < 50; i++)
                {
                    await repository.CreateAsync(new Person
                    {
                        FirstName = $"Sustained{i}",
                        LastName = "Test",
                        Age = 30,
                        Email = $"sustained{i}@test.com",
                        Salary = 50000m,
                        Department = "Sustained"
                    });
                }

                int iterations = 5;
                int queriesPerIteration = 200;
                List<int> timeoutsPerIteration = new List<int>();

                for (int iter = 0; iter < iterations; iter++)
                {
                    int timeouts = 0;
                    int successes = 0;

                    Task[] tasks = new Task[queriesPerIteration];
                    for (int q = 0; q < queriesPerIteration; q++)
                    {
                        tasks[q] = Task.Run(async () =>
                        {
                            try
                            {
                                int count = await repository.CountAsync();
                                Interlocked.Increment(ref successes);
                            }
                            catch (TimeoutException)
                            {
                                Interlocked.Increment(ref timeouts);
                            }
                        });
                    }

                    await Task.WhenAll(tasks);
                    timeoutsPerIteration.Add(timeouts);

                    Console.WriteLine($"  Iteration {iter + 1}: {successes} successes, {timeouts} timeouts");
                }

                int lastIterationTimeouts = timeoutsPerIteration.Last();
                Assert.True(lastIterationTimeouts == 0,
                    $"Expected no timeouts in final iteration (got {lastIterationTimeouts}), indicating possible leak");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        #endregion

        #region IDisposable

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public virtual void Dispose()
        {
        }

        #endregion
    }
}
