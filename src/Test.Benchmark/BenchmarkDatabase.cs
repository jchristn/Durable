namespace Test.Benchmark
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.IO;
    using Microsoft.Data.Sqlite;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Npgsql;

    /// <summary>
    /// Creates and seeds the benchmark database for one provider and hands out Durable repositories and raw connections.
    /// SQLite uses a file in a fresh temporary directory; PostgreSQL uses the connection string in
    /// <see cref="PostgresEnvironmentVariable"/> and drops/recreates the benchmark tables.
    /// </summary>
    public sealed class BenchmarkDatabase : IDisposable
    {
        /// <summary>
        /// Environment variable holding a PostgreSQL connection string; when set, PostgreSQL is benchmarked too.
        /// </summary>
        public const string PostgresEnvironmentVariable = "DURABLE_BENCH_POSTGRES";

        /// <summary>Number of order rows seeded.</summary>
        public const int OrderCount = 10000;

        /// <summary>Number of distinct customers (each has OrderCount / CustomerCount orders).</summary>
        public const int CustomerCount = 100;

        /// <summary>Number of authors seeded for the Include benchmark.</summary>
        public const int AuthorCount = 100;

        /// <summary>Number of posts per author.</summary>
        public const int PostsPerAuthor = 10;

        /// <summary>Gets the provider name ("sqlite" or "postgres").</summary>
        public string Provider { get; }

        /// <summary>Gets the order repository.</summary>
        public ISqlRepository<BenchOrder> Orders { get; }

        /// <summary>Gets the author repository.</summary>
        public ISqlRepository<BenchAuthor> Authors { get; }

        /// <summary>Gets the post repository.</summary>
        public ISqlRepository<BenchPost> Posts { get; }

        private readonly string _ConnectionString;
        private readonly string? _TempDirectory;
        private readonly IConnectionFactory _Factory;

        /// <summary>
        /// Creates and seeds the database.
        /// </summary>
        /// <param name="provider">"sqlite" or "postgres".</param>
        /// <exception cref="ArgumentException">Thrown for an unknown provider.</exception>
        /// <exception cref="InvalidOperationException">Thrown when postgres is requested without a connection string.</exception>
        public BenchmarkDatabase(string provider)
        {
            Provider = provider;
            if (provider == "sqlite")
            {
                _TempDirectory = Path.Combine(Path.GetTempPath(), "durable-bench-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(_TempDirectory);
                _ConnectionString = "Data Source=" + Path.Combine(_TempDirectory, "bench.db");
                _Factory = new SqliteConnectionFactory(_ConnectionString);
                Orders = new SqliteRepository<BenchOrder>(_Factory);
                Authors = new SqliteRepository<BenchAuthor>(_Factory);
                Posts = new SqliteRepository<BenchPost>(_Factory);
            }
            else if (provider == "postgres")
            {
                _ConnectionString = Environment.GetEnvironmentVariable(PostgresEnvironmentVariable)
                    ?? throw new InvalidOperationException(PostgresEnvironmentVariable + " is not set.");
                _Factory = new PostgresConnectionFactory(_ConnectionString);
                Orders = new PostgresRepository<BenchOrder>(_Factory);
                Authors = new PostgresRepository<BenchAuthor>(_Factory);
                Posts = new PostgresRepository<BenchPost>(_Factory);
                using (DbConnection connection = OpenConnection())
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS bench_posts, bench_authors, bench_orders";
                    command.ExecuteNonQuery();
                }
            }
            else
            {
                throw new ArgumentException("Unknown provider " + provider, nameof(provider));
            }

            Seed();
        }

        /// <summary>
        /// Opens a new raw ADO.NET connection (pooled by the driver), for the Dapper and hand-written baselines.
        /// </summary>
        /// <returns>An open connection; the caller disposes it.</returns>
        public DbConnection OpenConnection()
        {
            DbConnection connection = Provider == "sqlite"
                ? new SqliteConnection(_ConnectionString)
                : new NpgsqlConnection(_ConnectionString);
            connection.Open();
            return connection;
        }

        /// <summary>
        /// Disposes repositories and the factory and deletes the temporary SQLite directory.
        /// </summary>
        public void Dispose()
        {
            Orders.Dispose();
            Authors.Dispose();
            Posts.Dispose();
            if (_Factory is IDisposable disposable) disposable.Dispose();
            SqliteConnection.ClearAllPools();
            if (_TempDirectory != null)
            {
                try
                {
                    Directory.Delete(_TempDirectory, true);
                }
                catch (IOException)
                {
                }
            }
        }

        private void Seed()
        {
            Orders.InitializeTable(typeof(BenchOrder));
            Authors.InitializeTable(typeof(BenchAuthor));
            Posts.InitializeTable(typeof(BenchPost));

            Random random = new Random(42);
            DateTime start = new DateTime(2024, 1, 1, 8, 30, 0, DateTimeKind.Utc);
            OrderStatus[] statuses = (OrderStatus[])Enum.GetValues(typeof(OrderStatus));
            List<BenchOrder> orders = new List<BenchOrder>(OrderCount);
            for (int i = 0; i < OrderCount; i++)
            {
                long customer = (i % CustomerCount) + 1;
                orders.Add(new BenchOrder
                {
                    CustomerId = customer,
                    CustomerName = "Customer " + customer,
                    Notes = i % 2 == 0 ? "Leave at the front door, order " + i : null,
                    CreatedUtc = start.AddMinutes(i * 7),
                    Total = Math.Round((decimal)(random.NextDouble() * 1000), 2),
                    IsPaid = i % 3 != 0,
                    Status = statuses[i % statuses.Length],
                    Reference = Guid.NewGuid(),
                    Priority = i % 3 == 0 ? null : i % 5
                });
            }

            Orders.CreateMany(orders);

            for (int a = 0; a < AuthorCount; a++)
            {
                BenchAuthor author = Authors.Create(new BenchAuthor { Name = "Author " + a, Country = a % 4 == 0 ? null : "Country " + (a % 7) });
                List<BenchPost> posts = new List<BenchPost>(PostsPerAuthor);
                for (int p = 0; p < PostsPerAuthor; p++)
                {
                    posts.Add(new BenchPost { AuthorId = author.Id, Title = "Post " + p + " by author " + a, PublishedUtc = start.AddDays(p), Views = random.Next(0, 100000) });
                }

                Posts.CreateMany(posts);
            }
        }
    }
}
