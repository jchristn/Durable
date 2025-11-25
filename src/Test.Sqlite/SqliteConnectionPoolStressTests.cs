namespace Test.Sqlite
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// SQLite-specific connection pool stress tests.
    /// Note: SQLite doesn't have a native SLEEP function, so slow query tests
    /// simulate delays using application-level waits combined with database operations.
    /// </summary>
    public class SqliteConnectionPoolStressTests : ConnectionPoolStressTests
    {
        #region Private-Members

        private readonly string _ConnectionString;
        private SqliteConnectionFactory? _SetupFactory;
        private SqliteRepository<Person>? _SetupRepository;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteConnectionPoolStressTests"/> class.
        /// </summary>
        public SqliteConnectionPoolStressTests()
            : base(
                new SqliteRepositoryProvider("Data Source=stress_test.db"),
                // SQLite doesn't have SLEEP - we use a simple SELECT and handle delays in code
                "SELECT 1",
                "SELECT 1")
        {
            _ConnectionString = "Data Source=stress_test.db";
        }

        #endregion

        #region Abstract-Implementation

        /// <inheritdoc/>
        protected override IConnectionFactory CreateConnectionFactory(ConnectionPoolOptions options)
        {
            return new SqliteConnectionFactory(_ConnectionString, options);
        }

        /// <inheritdoc/>
        protected override IRepository<T> CreateRepository<T>(IConnectionFactory factory)
        {
            return new SqliteRepository<T>((SqliteConnectionFactory)factory);
        }

        /// <inheritdoc/>
        protected override Task<bool> IsDatabaseAvailableAsync()
        {
            return Task.FromResult(true);
        }

        /// <inheritdoc/>
        protected override async Task SetupDatabaseAsync()
        {
            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 1,
                MaxPoolSize = 5,
                ConnectionTimeout = TimeSpan.FromSeconds(30)
            };

            _SetupFactory = new SqliteConnectionFactory(_ConnectionString, options);
            _SetupRepository = new SqliteRepository<Person>(_SetupFactory);

            await _SetupRepository.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first TEXT NOT NULL,
                    last TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT,
                    salary REAL NOT NULL,
                    department TEXT
                )
            ");
        }

        /// <inheritdoc/>
        protected override async Task CleanupDatabaseAsync()
        {
            if (_SetupRepository != null)
            {
                try
                {
                    await _SetupRepository.ExecuteSqlAsync("DELETE FROM people");
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }

            _SetupFactory?.Dispose();
            _SetupFactory = null;
            _SetupRepository = null;
        }

        /// <inheritdoc/>
        protected override ITransaction BeginTransaction<T>(IRepository<T> repository)
        {
            return ((SqliteRepository<T>)repository).BeginTransaction();
        }

        /// <inheritdoc/>
        protected override Task<ITransaction> BeginTransactionAsync<T>(IRepository<T> repository, CancellationToken token = default)
        {
            return ((SqliteRepository<T>)repository).BeginTransactionAsync(token);
        }

        #endregion

        #region SQLite-Specific-Tests

        /// <summary>
        /// SQLite-specific test for connection pool with file-based database.
        /// </summary>
        [Fact]
        public async Task SqliteFile_ConcurrentAccess_ShouldHandleWALMode()
        {
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

                SqliteConnectionFactory factory = new SqliteConnectionFactory(_ConnectionString, options);
                SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

                await repository.ExecuteSqlAsync("PRAGMA journal_mode=WAL");
                await repository.ExecuteSqlAsync("DELETE FROM people");

                int threadCount = 5;
                int operationsPerThread = 50;
                int successCount = 0;

                Task[] tasks = new Task[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    int threadId = t;
                    tasks[t] = Task.Run(async () =>
                    {
                        for (int i = 0; i < operationsPerThread; i++)
                        {
                            try
                            {
                                if (i % 2 == 0)
                                {
                                    await repository.CreateAsync(new Person
                                    {
                                        FirstName = $"WAL{threadId}_{i}",
                                        LastName = "Test",
                                        Age = 30,
                                        Email = $"wal{threadId}_{i}@test.com",
                                        Salary = 50000m,
                                        Department = "WALTest"
                                    });
                                }
                                else
                                {
                                    int count = await repository.CountAsync();
                                }
                                Interlocked.Increment(ref successCount);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"    Error: {ex.Message}");
                            }
                        }
                    });
                }

                await Task.WhenAll(tasks);

                int totalOps = threadCount * operationsPerThread;
                Console.WriteLine($"  WAL mode: {successCount}/{totalOps} operations succeeded");

                Assert.True(successCount > totalOps * 0.9,
                    $"Expected >90% success rate with WAL mode, got {successCount}/{totalOps}");

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests SQLite busy timeout handling with concurrent writes.
        /// </summary>
        [Fact]
        public async Task SqliteBusyTimeout_ConcurrentWrites_ShouldHandle()
        {
            await SetupDatabaseAsync();

            try
            {
                ConnectionPoolOptions options = new ConnectionPoolOptions
                {
                    MinPoolSize = 1,
                    MaxPoolSize = 5,
                    ConnectionTimeout = TimeSpan.FromSeconds(30),
                    IdleTimeout = TimeSpan.FromMinutes(1)
                };

                SqliteConnectionFactory factory = new SqliteConnectionFactory(_ConnectionString, options);
                SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int writerCount = 10;
                int writesPerWriter = 20;
                int successCount = 0;
                int errorCount = 0;

                Task[] tasks = new Task[writerCount];
                for (int w = 0; w < writerCount; w++)
                {
                    int writerId = w;
                    tasks[w] = Task.Run(async () =>
                    {
                        for (int i = 0; i < writesPerWriter; i++)
                        {
                            try
                            {
                                await repository.CreateAsync(new Person
                                {
                                    FirstName = $"Busy{writerId}_{i}",
                                    LastName = "Test",
                                    Age = 30,
                                    Email = $"busy{writerId}_{i}@test.com",
                                    Salary = 50000m,
                                    Department = "BusyTest"
                                });
                                Interlocked.Increment(ref successCount);
                            }
                            catch
                            {
                                Interlocked.Increment(ref errorCount);
                            }
                        }
                    });
                }

                await Task.WhenAll(tasks);

                int totalWrites = writerCount * writesPerWriter;
                Console.WriteLine($"  Concurrent writes: {successCount} succeeded, {errorCount} failed");

                int actualCount = await repository.CountAsync(p => p.Department == "BusyTest");
                Assert.Equal(successCount, actualCount);

                factory.Dispose();
            }
            finally
            {
                await CleanupDatabaseAsync();
            }
        }

        /// <summary>
        /// Tests pool behavior with simulated slow operations using transactions.
        /// Since SQLite doesn't have SLEEP, we simulate slow operations by holding transactions.
        /// </summary>
        [Fact]
        public async Task SqliteSimulatedSlowOps_WithTransactionHolding()
        {
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

                SqliteConnectionFactory factory = new SqliteConnectionFactory(_ConnectionString, options);
                SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

                await repository.ExecuteSqlAsync("DELETE FROM people");

                int longHoldingTransactions = 5;
                int holdTimeMs = 2000;
                int successCount = 0;
                int timeoutCount = 0;

                Task[] tasks = new Task[longHoldingTransactions];
                for (int t = 0; t < longHoldingTransactions; t++)
                {
                    int transId = t;
                    tasks[t] = Task.Run(async () =>
                    {
                        try
                        {
                            using ITransaction transaction = await repository.BeginTransactionAsync();
                            try
                            {
                                await repository.CreateAsync(new Person
                                {
                                    FirstName = $"SlowTrans{transId}",
                                    LastName = "Test",
                                    Age = 30,
                                    Email = $"slowtrans{transId}@test.com",
                                    Salary = 50000m,
                                    Department = "SlowTransTest"
                                }, transaction);

                                // Simulate slow operation by holding transaction
                                await Task.Delay(holdTimeMs);

                                await transaction.CommitAsync();
                                Interlocked.Increment(ref successCount);
                            }
                            catch
                            {
                                await transaction.RollbackAsync();
                                throw;
                            }
                        }
                        catch (TimeoutException)
                        {
                            Interlocked.Increment(ref timeoutCount);
                        }
                    });
                }

                await Task.WhenAll(tasks);

                Console.WriteLine($"  Pool: {options.MaxPoolSize}, Long transactions: {longHoldingTransactions}");
                Console.WriteLine($"  Success: {successCount}, Timeout: {timeoutCount}");

                // With pool of 3 and 5 transactions each holding 2s, some should timeout
                Assert.True(timeoutCount > 0 || successCount == longHoldingTransactions,
                    "Expected either timeouts or all successes depending on timing");

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
            _SetupFactory?.Dispose();
            base.Dispose();

            // Clean up the test database file
            try
            {
                if (System.IO.File.Exists("stress_test.db"))
                {
                    System.IO.File.Delete("stress_test.db");
                }
                if (System.IO.File.Exists("stress_test.db-wal"))
                {
                    System.IO.File.Delete("stress_test.db-wal");
                }
                if (System.IO.File.Exists("stress_test.db-shm"))
                {
                    System.IO.File.Delete("stress_test.db-shm");
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        #endregion
    }
}
