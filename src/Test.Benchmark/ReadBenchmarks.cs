namespace Test.Benchmark
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using System.Threading.Tasks;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Configs;
    using Dapper;

    /// <summary>
    /// Read-path benchmarks: Durable vs Dapper vs hand-written ADO.NET. Every implementation opens a pooled connection per
    /// operation (as Durable does) and fully materializes the result.
    /// </summary>
    [MemoryDiagnoser]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    [CategoriesColumn]
    public class ReadBenchmarks
    {
        private const string OrderColumns = "id, customer_id, customer_name, notes, created_utc, total, is_paid, status, reference, priority";
        private const string SelectAll = "SELECT " + OrderColumns + " FROM bench_orders";
        private const string SelectById = SelectAll + " WHERE id = @Id";
        private const string SelectByCustomer = SelectAll + " WHERE customer_id = @CustomerId";
        private const string SelectSummaries = "SELECT id, customer_name, total, status FROM bench_orders";

        private BenchmarkDatabase _Database = null!;
        private int _NextId;

        /// <summary>
        /// Gets or sets the provider under test.
        /// </summary>
        [ParamsSource(nameof(Providers))]
        public string Provider { get; set; } = "sqlite";

        /// <summary>
        /// Gets the providers to benchmark: SQLite always, PostgreSQL when its environment variable is set.
        /// </summary>
        public static IEnumerable<string> Providers
        {
            get
            {
                yield return "sqlite";
                if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable(BenchmarkDatabase.PostgresEnvironmentVariable)))
                    yield return "postgres";
            }
        }

        /// <summary>
        /// Creates and seeds the database.
        /// </summary>
        [GlobalSetup]
        public void Setup()
        {
            SqlMapper.AddTypeHandler(new DapperGuidHandler());
            DefaultTypeMap.MatchNamesWithUnderscores = true;
            _Database = new BenchmarkDatabase(Provider);
        }

        /// <summary>
        /// Drops the database.
        /// </summary>
        [GlobalCleanup]
        public void Cleanup()
        {
            _Database.Dispose();
        }

        #region Read-by-id

        [Benchmark(Baseline = true), BenchmarkCategory("ReadById")]
        public BenchOrder? Ado_ReadById()
        {
            using DbConnection connection = _Database.OpenConnection();
            using DbCommand command = connection.CreateCommand();
            command.CommandText = SelectById;
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = "@Id";
            parameter.Value = NextId();
            command.Parameters.Add(parameter);
            using DbDataReader reader = command.ExecuteReader();
            return reader.Read() ? MapOrder(reader) : null;
        }

        [Benchmark, BenchmarkCategory("ReadById")]
        public BenchOrder? Dapper_ReadById()
        {
            using DbConnection connection = _Database.OpenConnection();
            return connection.QueryFirstOrDefault<BenchOrder>(SelectById, new { Id = NextId() });
        }

        [Benchmark, BenchmarkCategory("ReadById")]
        public BenchOrder? Durable_ReadById()
        {
            return _Database.Orders.ReadById(NextId());
        }

        [Benchmark, BenchmarkCategory("ReadById")]
        public BenchOrder? Durable_QueryWhereId()
        {
            int id = NextId();
            return _Database.Orders.Query().Where(x => x.Id == id).Execute().FirstOrDefault();
        }

        #endregion

        #region Read-all

        [Benchmark(Baseline = true), BenchmarkCategory("ReadAll")]
        public List<BenchOrder> Ado_ReadAll()
        {
            using DbConnection connection = _Database.OpenConnection();
            using DbCommand command = connection.CreateCommand();
            command.CommandText = SelectAll;
            using DbDataReader reader = command.ExecuteReader();
            List<BenchOrder> list = new List<BenchOrder>();
            while (reader.Read()) list.Add(MapOrder(reader));
            return list;
        }

        [Benchmark, BenchmarkCategory("ReadAll")]
        public List<BenchOrder> Dapper_ReadAll()
        {
            using DbConnection connection = _Database.OpenConnection();
            return connection.Query<BenchOrder>(SelectAll).AsList();
        }

        [Benchmark, BenchmarkCategory("ReadAll")]
        public List<BenchOrder> Durable_ReadAll()
        {
            return _Database.Orders.ReadAll().ToList();
        }

        [Benchmark(Baseline = true), BenchmarkCategory("ReadAllAsync")]
        public async Task<List<BenchOrder>> Ado_ReadAllAsync()
        {
            await using DbConnection connection = _Database.OpenConnection();
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = SelectAll;
            await using DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            List<BenchOrder> list = new List<BenchOrder>();
            while (await reader.ReadAsync().ConfigureAwait(false)) list.Add(MapOrder(reader));
            return list;
        }

        [Benchmark, BenchmarkCategory("ReadAllAsync")]
        public async Task<List<BenchOrder>> Dapper_ReadAllAsync()
        {
            await using DbConnection connection = _Database.OpenConnection();
            return (await connection.QueryAsync<BenchOrder>(SelectAll).ConfigureAwait(false)).AsList();
        }

        [Benchmark, BenchmarkCategory("ReadAllAsync")]
        public async Task<List<BenchOrder>> Durable_ReadAllAsyncEnumerable()
        {
            List<BenchOrder> list = new List<BenchOrder>();
            await foreach (BenchOrder order in _Database.Orders.Query().ExecuteAsyncEnumerable().ConfigureAwait(false)) list.Add(order);
            return list;
        }

        #endregion

        #region Filtered

        [Benchmark(Baseline = true), BenchmarkCategory("Filtered100")]
        public List<BenchOrder> Ado_Filtered()
        {
            using DbConnection connection = _Database.OpenConnection();
            using DbCommand command = connection.CreateCommand();
            command.CommandText = SelectByCustomer;
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = "@CustomerId";
            parameter.Value = NextCustomer();
            command.Parameters.Add(parameter);
            using DbDataReader reader = command.ExecuteReader();
            List<BenchOrder> list = new List<BenchOrder>();
            while (reader.Read()) list.Add(MapOrder(reader));
            return list;
        }

        [Benchmark, BenchmarkCategory("Filtered100")]
        public List<BenchOrder> Dapper_Filtered()
        {
            using DbConnection connection = _Database.OpenConnection();
            return connection.Query<BenchOrder>(SelectByCustomer, new { CustomerId = NextCustomer() }).AsList();
        }

        [Benchmark, BenchmarkCategory("Filtered100")]
        public List<BenchOrder> Durable_ReadMany()
        {
            long customer = NextCustomer();
            return _Database.Orders.ReadMany(x => x.CustomerId == customer).ToList();
        }

        #endregion

        #region DTO

        [Benchmark(Baseline = true), BenchmarkCategory("Dto")]
        public List<OrderSummary> Ado_Dto()
        {
            using DbConnection connection = _Database.OpenConnection();
            using DbCommand command = connection.CreateCommand();
            command.CommandText = SelectSummaries;
            using DbDataReader reader = command.ExecuteReader();
            List<OrderSummary> list = new List<OrderSummary>();
            while (reader.Read())
            {
                list.Add(new OrderSummary
                {
                    Id = reader.GetInt32(0),
                    CustomerName = reader.GetString(1),
                    Total = reader.GetDecimal(2),
                    Status = Enum.Parse<OrderStatus>(reader.GetString(3))
                });
            }

            return list;
        }

        [Benchmark, BenchmarkCategory("Dto")]
        public List<OrderSummary> Dapper_Dto()
        {
            using DbConnection connection = _Database.OpenConnection();
            return connection.Query<OrderSummary>(SelectSummaries).AsList();
        }

        [Benchmark, BenchmarkCategory("Dto")]
        public List<OrderSummary> Durable_FromSqlDto()
        {
            return _Database.Orders.FromSql<OrderSummary>(SelectSummaries).ToList();
        }

        [Benchmark, BenchmarkCategory("Dto")]
        public List<OrderSummary> Durable_SelectProjection()
        {
            return _Database.Orders.Query()
                .Select(x => new OrderSummary { Id = x.Id, CustomerName = x.CustomerName, Total = x.Total, Status = x.Status })
                .Execute()
                .ToList();
        }

        #endregion

        #region Include

        [Benchmark(Baseline = true), BenchmarkCategory("Include")]
        public List<BenchAuthor> Ado_Include()
        {
            using DbConnection connection = _Database.OpenConnection();
            List<BenchAuthor> authors = new List<BenchAuthor>();
            Dictionary<int, BenchAuthor> byId = new Dictionary<int, BenchAuthor>();
            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT id, name, country FROM bench_authors";
                using DbDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    BenchAuthor author = new BenchAuthor { Id = reader.GetInt32(0), Name = reader.GetString(1), Country = reader.IsDBNull(2) ? null : reader.GetString(2) };
                    authors.Add(author);
                    byId[author.Id] = author;
                }
            }

            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT id, author_id, title, published_utc, views FROM bench_posts ORDER BY id";
                using DbDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    BenchPost post = new BenchPost { Id = reader.GetInt32(0), AuthorId = reader.GetInt32(1), Title = reader.GetString(2), PublishedUtc = reader.GetDateTime(3), Views = reader.GetInt32(4) };
                    if (byId.TryGetValue(post.AuthorId, out BenchAuthor? owner)) owner.Posts.Add(post);
                }
            }

            return authors;
        }

        [Benchmark, BenchmarkCategory("Include")]
        public List<BenchAuthor> Dapper_Include()
        {
            using DbConnection connection = _Database.OpenConnection();
            List<BenchAuthor> authors = connection.Query<BenchAuthor>("SELECT id, name, country FROM bench_authors").AsList();
            Dictionary<int, BenchAuthor> byId = authors.ToDictionary(a => a.Id);
            int[] ids = byId.Keys.ToArray();
            string sql = Provider == "postgres"
                ? "SELECT id, author_id, title, published_utc, views FROM bench_posts WHERE author_id = ANY(@Ids) ORDER BY id"
                : "SELECT id, author_id, title, published_utc, views FROM bench_posts WHERE author_id IN @Ids ORDER BY id";
            foreach (BenchPost post in connection.Query<BenchPost>(sql, new { Ids = ids }))
            {
                if (byId.TryGetValue(post.AuthorId, out BenchAuthor? owner)) owner.Posts.Add(post);
            }

            return authors;
        }

        [Benchmark, BenchmarkCategory("Include")]
        public List<BenchAuthor> Durable_Include()
        {
            return _Database.Authors.Query().Include(a => a.Posts).Execute().ToList();
        }

        #endregion

        private static BenchOrder MapOrder(DbDataReader reader)
        {
            return new BenchOrder
            {
                Id = reader.GetInt32(0),
                CustomerId = reader.GetInt64(1),
                CustomerName = reader.GetString(2),
                Notes = reader.IsDBNull(3) ? null : reader.GetString(3),
                CreatedUtc = reader.GetDateTime(4),
                Total = reader.GetDecimal(5),
                IsPaid = reader.GetBoolean(6),
                Status = Enum.Parse<OrderStatus>(reader.GetString(7)),
                Reference = reader.GetGuid(8),
                Priority = reader.IsDBNull(9) ? null : reader.GetInt32(9)
            };
        }

        private int NextId()
        {
            _NextId = _NextId % BenchmarkDatabase.OrderCount + 1;
            return _NextId;
        }

        private long NextCustomer()
        {
            return NextId() % BenchmarkDatabase.CustomerCount + 1;
        }
    }
}
