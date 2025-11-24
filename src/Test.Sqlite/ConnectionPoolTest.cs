namespace Test.Sqlite
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// SQLite-specific connection pool tests.
    /// Tests connection pooling functionality specific to SQLite implementation.
    /// </summary>
    public class ConnectionPoolTest : IDisposable
    {
        #region Private-Members

        private readonly string _ConnectionString;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionPoolTest"/> class.
        /// </summary>
        public ConnectionPoolTest()
        {
            _ConnectionString = "Data Source=:memory:;Cache=Shared";
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests that SQLite repositories properly use connection pooling.
        /// </summary>
        [Fact]
        public async Task SqliteRepository_ShouldUseConnectionPool()
        {
            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 2,
                MaxPoolSize = 10,
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                IdleTimeout = TimeSpan.FromMinutes(2),
                ValidateConnections = true
            };

            SqliteConnectionFactory factory = new SqliteConnectionFactory(_ConnectionString, options);
            SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

            await repository.ExecuteSqlAsync(@"
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

            await repository.ExecuteSqlAsync("DELETE FROM people");

            for (int i = 0; i < 100; i++)
            {
                Person person = new Person
                {
                    FirstName = $"Person{i}",
                    LastName = $"Test{i}",
                    Age = 25 + (i % 40),
                    Email = $"person{i}@example.com",
                    Salary = 50000m,
                    Department = "Testing"
                };

                await repository.CreateAsync(person);
            }

            int count = await repository.CountAsync();
            Assert.Equal(100, count);

            factory.Dispose();
        }

        /// <summary>
        /// Tests SQLite connection pool performance under load.
        /// </summary>
        [Fact]
        public async Task SqliteConnectionPool_ShouldHandleHighLoad()
        {
            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 5,
                MaxPoolSize = 20,
                ConnectionTimeout = TimeSpan.FromSeconds(10),
                IdleTimeout = TimeSpan.FromMinutes(5),
                ValidateConnections = true
            };

            SqliteConnectionFactory factory = new SqliteConnectionFactory(_ConnectionString, options);
            SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

            await repository.ExecuteSqlAsync(@"
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

            await repository.ExecuteSqlAsync("DELETE FROM people");

            Stopwatch stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < 1000; i++)
            {
                Person person = new Person
                {
                    FirstName = $"Load{i}",
                    LastName = $"Test{i}",
                    Age = 30,
                    Email = $"load{i}@example.com",
                    Salary = 60000m,
                    Department = "Load"
                };

                Person created = await repository.CreateAsync(person);
                Assert.NotNull(created);
            }

            stopwatch.Stop();

            int count = await repository.CountAsync();
            Assert.Equal(1000, count);

            Console.WriteLine($"  Completed 1000 operations in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Average: {stopwatch.ElapsedMilliseconds / 1000.0:F2}ms per operation");

            factory.Dispose();
        }

        /// <summary>
        /// Tests that connections are properly released back to the pool.
        /// </summary>
        [Fact]
        public async Task SqliteConnectionPool_ShouldReleaseConnections()
        {
            ConnectionPoolOptions options = new ConnectionPoolOptions
            {
                MinPoolSize = 2,
                MaxPoolSize = 5,
                ConnectionTimeout = TimeSpan.FromSeconds(30),
                IdleTimeout = TimeSpan.FromMinutes(1),
                ValidateConnections = true
            };

            SqliteConnectionFactory factory = new SqliteConnectionFactory(_ConnectionString, options);
            SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

            await repository.ExecuteSqlAsync(@"
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

            await repository.ExecuteSqlAsync("DELETE FROM people");

            Person testPerson = new Person
            {
                FirstName = "Release",
                LastName = "Test",
                Age = 35,
                Email = "release@example.com",
                Salary = 55000m,
                Department = "Testing"
            };

            await repository.CreateAsync(testPerson);

            for (int cycle = 0; cycle < 20; cycle++)
            {
                for (int i = 0; i < 50; i++)
                {
                    Person? person = await repository.ReadFirstAsync(p => p.Email == "release@example.com");
                    Assert.NotNull(person);
                }

                await Task.Delay(10);
            }

            factory.Dispose();
        }

        /// <summary>
        /// Disposes resources used by the test.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
