namespace Test.Postgres
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// PostgreSQL-specific connection pool tests.
    /// Tests connection pooling functionality specific to PostgreSQL implementation.
    /// </summary>
    [Collection("Postgres Database Collection")]
    public class PostgresConnectionPoolTests : IDisposable
    {
        #region Private-Members

        private readonly PostgresRepositoryProvider _Provider;
        private readonly string _ConnectionString;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgresConnectionPoolTests"/> class.
        /// </summary>
        public PostgresConnectionPoolTests()
        {
            _ConnectionString = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
                ?? "Host=localhost;Database=durable_test;Username=test_user;Password=test_password;";
            _Provider = new PostgresRepositoryProvider(_ConnectionString);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests that PostgreSQL repositories properly use connection pooling.
        /// </summary>
        [Fact]
        public async Task PostgresRepository_ShouldUseConnectionPool()
        {
            bool isAvailable = await _Provider.IsDatabaseAvailableAsync();
            if (!isAvailable)
            {
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");
            }

            await _Provider.SetupDatabaseAsync();

            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 3,
                MaxPoolSize = 15,
                ConnectionTimeout = TimeSpan.FromSeconds(10),
                IdleTimeout = TimeSpan.FromMinutes(3),
                ValidateConnections = true
            };

            PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
            PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

            await repository.ExecuteSqlAsync("DELETE FROM people");

            for (int i = 0; i < 150; i++)
            {
                Person person = new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = 25 + (i % 40),
                    Email = $"person{i}@postgres.com",
                    Salary = 50000m,
                    Department = "Testing"
                };

                await repository.CreateAsync(person);
            }

            int count = await repository.CountAsync();
            Assert.Equal(150, count);

            factory.Dispose();
            await _Provider.CleanupDatabaseAsync();
        }

        /// <summary>
        /// Tests PostgreSQL connection pool performance under high load.
        /// </summary>
        [Fact]
        public async Task PostgresConnectionPool_ShouldHandleHighLoad()
        {
            bool isAvailable = await _Provider.IsDatabaseAvailableAsync();
            if (!isAvailable)
            {
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");
            }

            await _Provider.SetupDatabaseAsync();

            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 5,
                MaxPoolSize = 25,
                ConnectionTimeout = TimeSpan.FromSeconds(15),
                IdleTimeout = TimeSpan.FromMinutes(5),
                ValidateConnections = true
            };

            PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
            PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

            await repository.ExecuteSqlAsync("DELETE FROM people");

            Stopwatch stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < 1500; i++)
            {
                Person person = new Person
                {
                    FirstName = $"Load{i}",
                    LastName = $"Test{i}",
                    Age = 30,
                    Email = $"load{i}@postgres.com",
                    Salary = 60000m,
                    Department = "Load"
                };

                Person created = await repository.CreateAsync(person);
                Assert.NotNull(created);

                if (i % 5 == 0)
                {
                    Person[] people = (await repository.Query()
                        .Where(p => p.Department == "Load")
                        .Take(10)
                        .ExecuteAsync())
                        .ToArray();

                    Assert.NotEmpty(people);
                }
            }

            stopwatch.Stop();

            int count = await repository.CountAsync();
            Assert.True(count >= 1500);

            Console.WriteLine($"  Completed 1500+ operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Average: {stopwatch.ElapsedMilliseconds / 1500.0:F2}ms per operation");

            factory.Dispose();
            await _Provider.CleanupDatabaseAsync();
        }

        /// <summary>
        /// Tests concurrent access to PostgreSQL connection pool.
        /// </summary>
        [Fact]
        public async Task PostgresConnectionPool_ShouldHandleConcurrentAccess()
        {
            bool isAvailable = await _Provider.IsDatabaseAvailableAsync();
            if (!isAvailable)
            {
                throw new SharedTestRunner.SkipException("PostgreSQL database is not available");
            }

            await _Provider.SetupDatabaseAsync();

            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 5,
                MaxPoolSize = 20,
                ConnectionTimeout = TimeSpan.FromSeconds(30),
                IdleTimeout = TimeSpan.FromMinutes(2),
                ValidateConnections = true
            };

            PostgresConnectionFactory factory = new PostgresConnectionFactory(_ConnectionString, options);
            PostgresRepository<Person> repository = new PostgresRepository<Person>(factory);

            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person[] initialData = new Person[100];
            for (int i = 0; i < initialData.Length; i++)
            {
                initialData[i] = new Person
                {
                    FirstName = $"Concurrent{i}",
                    LastName = $"Test{i}",
                    Age = 25 + (i % 40),
                    Email = $"concurrent{i}@postgres.com",
                    Salary = 50000m,
                    Department = "Concurrent"
                };
            }

            await repository.CreateManyAsync(initialData);

            int threadCount = 10;
            int queriesPerThread = 200;

            Task[] tasks = new Task[threadCount];
            for (int t = 0; t < threadCount; t++)
            {
                tasks[t] = Task.Run(async () =>
                {
                    for (int i = 0; i < queriesPerThread; i++)
                    {
                        Person[] people = (await repository.Query()
                            .Where(p => p.Department == "Concurrent")
                            .Take(5)
                            .ExecuteAsync())
                            .ToArray();

                        Assert.NotEmpty(people);
                    }
                });
            }

            await Task.WhenAll(tasks);

            Console.WriteLine($"  Completed {threadCount * queriesPerThread:N0} concurrent queries");

            factory.Dispose();
            await _Provider.CleanupDatabaseAsync();
        }

        /// <summary>
        /// Disposes resources used by the test.
        /// </summary>
        public void Dispose()
        {
            _Provider?.Dispose();
        }

        #endregion
    }
}
