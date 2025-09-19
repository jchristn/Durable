namespace Test.Sqlite
{
    using System;
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

            // Create connection factory with custom pool options
            IConnectionFactory factory = connectionString.CreateFactory(options =>
            {
                options.MinPoolSize = 3;
                options.MaxPoolSize = 20;
                options.ConnectionTimeout = TimeSpan.FromSeconds(10);
                options.IdleTimeout = TimeSpan.FromMinutes(5);
                options.ValidateConnections = true;
            });

            // Create repository using the pooled factory
            using SqliteRepository<Person> repository = new SqliteRepository<Person>(factory);

            // Simulate multiple concurrent operations
            Task[] tasks = new Task[10];
            for (int i = 0; i < 10; i++)
            {
                int personId = i + 1;
                tasks[i] = Task.Run(async () =>
                {
                    Console.WriteLine($"Task {personId}: Starting database operations");

                    // Each operation will reuse connections from the pool
                    Person person = new Person
                    {
                        FirstName = $"Person",
                        LastName = $"{personId}",
                        Age = 20 + personId,
                        Email = $"person{personId}@example.com",
                        Salary = 50000,
                        Department = "IT"
                    };

                    await repository.CreateAsync(person);
                    Console.WriteLine($"Task {personId}: Created person with ID {person.Id}");

                    Person retrieved = await repository.ReadByIdAsync(person.Id);
                    Console.WriteLine($"Task {personId}: Retrieved person: {retrieved.Name}");

                    await Task.Delay(100); // Simulate work

                    Console.WriteLine($"Task {personId}: Completed database operations");
                });
            }

            Task.WaitAll(tasks);

            // Cleanup
            repository.DeleteAll();
            factory.Dispose();

            Console.WriteLine("Connection pooling example completed successfully!");
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
                options.MinPoolSize = 5;
                options.MaxPoolSize = 20; // Reduce from default 100 to prevent semaphore exhaustion
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