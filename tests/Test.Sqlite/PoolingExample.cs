namespace Test.Sqlite
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sqlite;
    using Test.Shared;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Demonstrates connection pooling functionality and performance comparisons.
    /// </summary>
    public static class PoolingExample
    {
        /// <summary>
        /// Demonstrates connection pooling capabilities with performance monitoring.
        /// </summary>
        public static void DemonstrateConnectionPooling()
        {
            Console.WriteLine("=== Connection Pooling Example ===");

            string connectionString = "Data Source=pooling_example.db";

            // Initialize database
            InitializeDatabase(connectionString);

            // Create connection factory with conservative pool options for SQLite
            IConnectionFactory factory = connectionString.CreateFactory(options =>
            {
                options.MinPoolSize = 1;
                options.MaxPoolSize = 2;  // Very conservative for SQLite
                options.ConnectionTimeout = TimeSpan.FromSeconds(30);
                options.IdleTimeout = TimeSpan.FromMinutes(5);
                options.ValidateConnections = true;
            });

            Console.WriteLine("Testing basic connection pooling functionality...");

            // Test 1: Sequential operations with explicit connection management
            Console.WriteLine("\n1. Testing sequential operations with connection reuse:");

            // Use a single repository instance to avoid factory disposal issues
            using (SqliteRepository<Person> repository = new SqliteRepository<Person>(factory))
            {
                for (int i = 1; i <= 5; i++)
                {
                    Console.WriteLine($"  Operation {i}: Creating person");

                    Person person = new Person
                    {
                        FirstName = $"Test",
                        LastName = $"{i}",
                        Age = 20 + i,
                        Email = $"test{i}@pooling.com",
                        Salary = 50000,
                        Department = "IT"
                    };

                    Person created = repository.Create(person);
                    Person retrieved = repository.ReadById(created.Id);

                    Console.WriteLine($"  Operation {i}: Success - Created and retrieved {retrieved?.Name}");

                    // Small delay to simulate real-world usage
                    Thread.Sleep(50);
                }
            }

            // Test 2: Verify pool reuse by checking performance
            Console.WriteLine("\n2. Testing connection pool performance benefit:");

            System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();

            // Create a new factory for this test since the previous repository disposed the shared one
            IConnectionFactory perfFactory = connectionString.CreateFactory(options =>
            {
                options.MinPoolSize = 1;
                options.MaxPoolSize = 2;
                options.ConnectionTimeout = TimeSpan.FromSeconds(30);
                options.IdleTimeout = TimeSpan.FromMinutes(5);
                options.ValidateConnections = true;
            });

            using (SqliteRepository<Person> repository = new SqliteRepository<Person>(perfFactory))
            {
                for (int i = 1; i <= 10; i++)
                {
                    Person person = new Person
                    {
                        FirstName = $"Perf",
                        LastName = $"{i}",
                        Age = 30,
                        Email = $"perf{i}@pooling.com",
                        Salary = 60000,
                        Department = "Performance"
                    };

                    repository.Create(person);
                }

                sw.Stop();
                Console.WriteLine($"  Completed 10 operations in {sw.ElapsedMilliseconds}ms (using connection pool)");

                // Cleanup within the same repository instance
                repository.DeleteAll();
            }
            // perfFactory is automatically disposed when repository is disposed

            Console.WriteLine("\n✅ Connection pooling example completed successfully!");
        }

        /// <summary>
        /// Compares performance between pooled and non-pooled connection strategies.
        /// </summary>
        public static void ComparePerformance()
        {
            Console.WriteLine("=== Performance Comparison ===");

            string connectionString = "Data Source=performance_test.db";
            const int operationCount = 100;

            // Initialize database
            InitializeDatabase(connectionString);

            // Test without pooling (traditional approach)
            System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
            long traditionalTime;
            using (SqliteRepository<Person> traditionalRepo = new SqliteRepository<Person>(connectionString))
            {
                for (int i = 0; i < operationCount; i++)
                {
                    Person person = new Person { FirstName = "Person", LastName = $"{i}", Age = 25, Email = $"test{i}@example.com", Salary = 50000, Department = "IT" };
                    traditionalRepo.Create(person);
                }

                stopwatch.Stop();
                traditionalTime = stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"Without pooling: {traditionalTime}ms for {operationCount} operations");

                // Clean up
                traditionalRepo.DeleteAll();
            }

            // Test with pooling
            stopwatch.Restart();
            using IConnectionFactory factory = connectionString.CreateFactory(options =>
            {
                options.MinPoolSize = 2;
                options.MaxPoolSize = 5; // Increased to handle sequential operations better
                options.ConnectionTimeout = TimeSpan.FromSeconds(30);
                options.IdleTimeout = TimeSpan.FromMinutes(5);
                options.ValidateConnections = true;
            });
            using SqliteRepository<Person> pooledRepo = new SqliteRepository<Person>(factory);

            for (int i = 0; i < operationCount; i++)
            {
                Person person = new Person { FirstName = "Person", LastName = $"{i}", Age = 25, Email = $"test{i}@example.com", Salary = 50000, Department = "IT" };
                pooledRepo.Create(person);
            }

            stopwatch.Stop();
            long pooledTime = stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"With pooling: {pooledTime}ms for {operationCount} operations");

            double improvement = ((double)(traditionalTime - pooledTime) / traditionalTime) * 100;
            Console.WriteLine($"Performance improvement: {improvement:F1}%");

            // Clean up
            pooledRepo.DeleteAll();
        }

        private static void InitializeDatabase(string connectionString)
        {
            using SqliteConnection connection = new SqliteConnection(connectionString);
            connection.Open();

            // Create table
            string createTableSql = @"
                CREATE TABLE IF NOT EXISTS people (
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
        }
    }
}