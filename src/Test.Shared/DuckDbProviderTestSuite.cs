namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.IO;
    using System.Linq;
    using System.Numerics;
    using System.Threading.Tasks;
    using Durable;
    using Durable.DuckDb;
    using Durable.Sql;
    using DuckDB.NET.Data;
    using Xunit;

    /// <summary>
    /// DuckDB-specific behavior: settings, in-memory and file database lifetime, native type round-tripping, the Appender
    /// bulk insert and its fallback, sequence-backed keys, optimistic concurrency conflicts, the row-based migration lock,
    /// and schema changes on indexed tables.
    /// </summary>
    public class DuckDbProviderTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the suite.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public DuckDbProviderTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Settings parse and build DuckDB.NET connection strings; the factories and validation behave as documented.
        /// </summary>
        [Fact]
        public void Settings_ParseBuildAndFactories()
        {
            DuckDbRepositorySettings parsed = DuckDbRepositorySettings.Parse("Data Source=analytics.duckdb;threads=2;memory_limit=1GB;access_mode=READ_ONLY");
            Assert.Equal(RepositoryType.DuckDb, parsed.Type);
            Assert.Equal("analytics.duckdb", parsed.DataSource);
            Assert.Equal(2, parsed.Threads);
            Assert.Equal("1GB", parsed.MemoryLimit);
            Assert.Equal(DuckDBAccessMode.ReadOnly, parsed.AccessMode);
            Assert.False(parsed.IsInMemory);

            DuckDbRepositorySettings again = DuckDbRepositorySettings.Parse(parsed.BuildConnectionString());
            Assert.Equal(parsed.DataSource, again.DataSource);
            Assert.Equal(parsed.Threads, again.Threads);
            Assert.Equal(parsed.AccessMode, again.AccessMode);

            Assert.True(DuckDbRepositorySettings.ForInMemory().IsInMemory);
            Assert.True(DuckDbRepositorySettings.ForSharedInMemory().IsInMemory);
            Assert.Equal("data.duckdb", DuckDbRepositorySettings.ForFile("data.duckdb").DataSource);
            Assert.Throws<ArgumentException>(() => DuckDbRepositorySettings.ForFile(" "));
            Assert.Throws<ArgumentException>(() => DuckDbRepositorySettings.Parse(" "));
            Assert.Throws<ArgumentException>(() => DuckDbRepositorySettings.Parse("Data Source=x.duckdb;no_such_option=1"));
            Assert.Throws<InvalidOperationException>(() => new DuckDbRepositorySettings().BuildConnectionString());
            Assert.Throws<InvalidOperationException>(() => new DuckDbRepositorySettings { DataSource = "x.duckdb", Threads = 0 }.BuildConnectionString());

            using DuckDbConnectionFactory memory = new DuckDbConnectionFactory(DuckDbRepositorySettings.ForInMemory());
            Assert.True(memory.IsInMemory);
            Assert.True(memory.IsPrivateInMemory);
            using DuckDbConnectionFactory shared = new DuckDbConnectionFactory(DuckDbRepositorySettings.ForSharedInMemory());
            Assert.True(shared.IsInMemory);
            Assert.False(shared.IsPrivateInMemory);
        }

        /// <summary>
        /// A ":memory:" database is shared by every connection and repository of one factory, private to that factory, and
        /// released when the factory is disposed.
        /// </summary>
        [Fact]
        public async Task PrivateInMemory_SharedWithinFactoryIsolatedAcrossFactories()
        {
            DuckDbConnectionFactory first = new DuckDbConnectionFactory("Data Source=:memory:");
            await using (DuckDbConnectionFactory second = new DuckDbConnectionFactory("Data Source=:memory:"))
            {
                using (DuckDbRepository<DuckTypesItem> a = new DuckDbRepository<DuckTypesItem>(first))
                using (DuckDbRepository<DuckTypesItem> b = new DuckDbRepository<DuckTypesItem>(first))
                using (DuckDbRepository<DuckTypesItem> other = new DuckDbRepository<DuckTypesItem>(second))
                {
                    a.InitializeTable(typeof(DuckTypesItem));
                    await a.CreateAsync(NewItem("shared"));
                    Assert.Equal(1, await b.CountAsync());
                    Assert.False(new DatabaseSchemaReader(second, DuckDbDialect.Default).TableExists("duck_types_items"));
                    Assert.True(new DatabaseSchemaReader(first, DuckDbDialect.Default).TableExists("duck_types_items"));
                }
            }

            first.Dispose();
            Assert.Throws<ObjectDisposedException>(() => first.OpenConnection());
        }

        /// <summary>
        /// A file database is created (with its directory) and keeps its data across factories.
        /// </summary>
        [Fact]
        public async Task FileDatabase_PersistsAcrossFactories()
        {
            string directory = Path.Combine(Path.GetTempPath(), "durable-duckdb-" + Guid.NewGuid().ToString("N"));
            string path = Path.Combine(directory, "nested", "file.duckdb");
            try
            {
                using (DuckDbRepository<DuckTypesItem> repository = new DuckDbRepository<DuckTypesItem>(DuckDbRepositorySettings.ForFile(path)))
                {
                    await repository.CreateDatabaseIfNotExistsAsync();
                    Assert.True(File.Exists(path));
                    repository.InitializeTable(typeof(DuckTypesItem));
                    await repository.CreateAsync(NewItem("persisted"));
                }

                using (DuckDbRepository<DuckTypesItem> reopened = new DuckDbRepository<DuckTypesItem>(DuckDbRepositorySettings.ForFile(path)))
                {
                    DuckTypesItem? row = await reopened.ReadFirstAsync(x => x.Name == "persisted");
                    Assert.NotNull(row);
                    Assert.Equal(1, row!.Id);
                }
            }
            finally
            {
                if (Directory.Exists(directory)) Directory.Delete(directory, true);
            }
        }

        /// <summary>
        /// Native DuckDB types round-trip: unsigned integers, UUID, TIMESTAMPTZ, TIMESTAMP, INTERVAL, DATE, TIME, DECIMAL,
        /// BLOB and JSON, with the column types the dialect declares.
        /// </summary>
        [Fact]
        public async Task NativeTypes_RoundTripWithDeclaredTypes()
        {
            ISqlRepository<DuckTypesItem> repository = _Provider.CreateRepository<DuckTypesItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            DuckTypesItem item = NewItem("types");
            item.Large = ulong.MaxValue;
            item.Medium = uint.MaxValue;
            item.Small = ushort.MaxValue;
            item.Tiny = byte.MaxValue;
            item.SignedTiny = sbyte.MinValue;
            DuckTypesItem created = await repository.CreateAsync(item);
            Assert.True(created.Id > 0);

            DuckTypesItem? read = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(read);
            Assert.Equal(item.Large, read!.Large);
            Assert.Equal(item.Medium, read.Medium);
            Assert.Equal(item.Small, read.Small);
            Assert.Equal(item.Tiny, read.Tiny);
            Assert.Equal(item.SignedTiny, read.SignedTiny);
            Assert.Equal(item.Uid, read.Uid);
            Assert.Equal(item.At.UtcDateTime, read.At.UtcDateTime);
            Assert.Equal(item.Stamp, read.Stamp);
            Assert.Equal(item.Span, read.Span);
            Assert.Equal(item.Day, read.Day);
            Assert.Equal(item.Clock, read.Clock);
            Assert.Equal(item.Price, read.Price);
            Assert.Equal(item.Data, read.Data);
            Assert.Equal(item.Tags, read.Tags);
            Assert.Equal(1, await repository.CountAsync(x => x.Large == ulong.MaxValue && x.Uid == item.Uid));

            TableSchema table = new DatabaseSchemaReader(repository.ConnectionFactory, repository.Dialect).ReadTable("duck_types_items")!;
            Assert.Equal("UBIGINT", table.FindColumn("large")!.DataType);
            Assert.Equal("UUID", table.FindColumn("uid")!.DataType);
            Assert.Equal("TIMESTAMP WITH TIME ZONE", table.FindColumn("at")!.DataType);
            Assert.Equal("INTERVAL", table.FindColumn("span")!.DataType);
            Assert.Equal("JSON", table.FindColumn("tags")!.DataType);
            Assert.True(table.FindColumn("id")!.IsAutoIncrement);

            Assert.Equal(new BigInteger(42), repository.ExecuteScalarRaw<BigInteger>("SELECT CAST(42 AS HUGEINT)"));
            Assert.Equal(42L, repository.ExecuteScalarRaw<long>("SELECT CAST(42 AS HUGEINT)"));
            Assert.Equal((decimal)item.Large, await repository.Query().SumAsync(x => x.Large));
        }

        /// <summary>
        /// BulkInsert uses the Appender for a table whose column types match (generating keys from the sequence) and falls
        /// back to a prepared INSERT when they do not; both honor the transaction.
        /// </summary>
        [Fact]
        public async Task BulkInsert_AppenderAndPreparedFallback()
        {
            ISqlRepository<DuckTypesItem> repository = _Provider.CreateRepository<DuckTypesItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            List<DuckTypesItem> rows = Enumerable.Range(0, 300).Select(i => NewItem("bulk-" + i)).ToList();
            Assert.Equal(300L, await repository.BulkInsertAsync(rows));
            List<DuckTypesItem> stored = (await repository.Query().ExecuteAsync()).ToList();
            Assert.Equal(300, stored.Count);
            Assert.Equal(Enumerable.Range(1, 300), stored.Select(x => x.Id).OrderBy(x => x));
            Assert.Equal(rows[17].Uid, stored.Single(x => x.Name == "bulk-17").Uid);

            await using (ISqlTransaction transaction = await repository.BeginTransactionAsync())
            {
                repository.BulkInsert(new[] { NewItem("rolled-back") }, transaction);
                Assert.Equal(301, await repository.CountAsync(null, transaction));
                await transaction.RollbackAsync();
            }

            Assert.Equal(300, await repository.CountAsync());
            DuckTypesItem next = await repository.CreateAsync(NewItem("after-bulk"));
            Assert.True(next.Id > 300);

            // A table whose types differ from the mapping (BIGINT / DOUBLE / VARCHAR instead of the native types) takes the
            // prepared INSERT path, which lets DuckDB convert each value.
            await repository.ExecuteSqlRawAsync(RelTestHelpers.DropTableSql(repository.Dialect, "duck_types_items"));
            await repository.ExecuteSqlRawAsync(
                "CREATE TABLE duck_types_items (id BIGINT PRIMARY KEY DEFAULT nextval('duck_types_items_id_seq'), name VARCHAR NOT NULL, tiny BIGINT, signed_tiny BIGINT, " +
                "small BIGINT, medium BIGINT, large DOUBLE, uid VARCHAR, \"at\" TIMESTAMPTZ, stamp TIMESTAMP, span INTERVAL, \"day\" DATE, clock TIME, price DOUBLE, data BLOB, tags VARCHAR)");
            Assert.Equal(25L, repository.BulkInsert(Enumerable.Range(0, 25).Select(i => NewItem("fallback-" + i))));
            Assert.Equal(25, await repository.CountAsync(x => x.Name.StartsWith("fallback-")));
            await RelTestHelpers.RecreateTableAsync(repository);
        }

        /// <summary>
        /// A dropped and recreated table numbers its rows from 1 again (its sequence is recreated with it).
        /// </summary>
        [Fact]
        public async Task RecreatedTable_RestartsSequence()
        {
            ISqlRepository<DuckTypesItem> repository = _Provider.CreateRepository<DuckTypesItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateAsync(NewItem("one"));
            await repository.CreateAsync(NewItem("two"));
            await RelTestHelpers.RecreateTableAsync(repository);
            Assert.Equal(1, (await repository.CreateAsync(NewItem("again"))).Id);
        }

        /// <summary>
        /// Concurrent transactions updating the same row: DuckDB fails the second writer with a conflict instead of waiting
        /// (optimistic concurrency), and the first writer's change stands.
        /// </summary>
        [Fact]
        public async Task ConcurrentUpdates_SecondWriterGetsConflict()
        {
            ISqlRepository<DuckTypesItem> repository = _Provider.CreateRepository<DuckTypesItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            DuckTypesItem row = await repository.CreateAsync(NewItem("contended"));

            await using ISqlTransaction first = await repository.BeginTransactionAsync();
            await using ISqlTransaction second = await repository.BeginTransactionAsync();
            Assert.Equal(1, await repository.UpdateFieldAsync(x => x.Id == row.Id, x => x.Medium, 1u, first));
            Exception conflict = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await repository.UpdateFieldAsync(x => x.Id == row.Id, x => x.Medium, 2u, second);
                await second.CommitAsync();
            });
            Assert.Contains("onflict", conflict.Message, StringComparison.Ordinal);
            await first.CommitAsync();
            Assert.Equal(1u, (await repository.ReadByIdAsync(row.Id))!.Medium);
        }

        /// <summary>
        /// The migration lock is a row in the lock table: a second session cannot take it while it is held, a row left by
        /// another process is treated as stale, and the lock table is not reported as a user table.
        /// </summary>
        [Fact]
        public async Task MigrationLock_RowLockSemantics()
        {
            DuckDbDialect dialect = DuckDbDialect.Default;
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await using DbConnection holder = await factory.OpenConnectionAsync();
            await using DbConnection contender = await factory.OpenConnectionAsync();
            const string name = "duck_lock_test";

            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(holder, dialect.AcquireMigrationLockSql(name, 0)!)));
            Assert.Null(await ScalarAsync(contender, dialect.AcquireMigrationLockSql(name, 0)!));
            await ScalarAsync(holder, dialect.ReleaseMigrationLockSql(name)!);
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(contender, dialect.AcquireMigrationLockSql(name, 0)!)));
            await ScalarAsync(contender, dialect.ReleaseMigrationLockSql(name)!);

            await ScalarAsync(holder, new SqlStatement("INSERT INTO " + dialect.QuoteIdentifier(dialect.MigrationLockTableName) + " VALUES ('" + name + "', 'exited-process', TIMESTAMP '2020-01-01 00:00:00')"));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(contender, dialect.AcquireMigrationLockSql(name, 0)!)));
            await ScalarAsync(contender, dialect.ReleaseMigrationLockSql(name)!);

            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, dialect);
            Assert.True(reader.TableExists(dialect.MigrationLockTableName));
            Assert.DoesNotContain(dialect.MigrationLockTableName, reader.ReadTableNames());
            await ScalarAsync(holder, dialect.AcquireMigrationLockSql(name, 0)!);
            string insertHeld = "INSERT INTO " + dialect.QuoteIdentifier(dialect.MigrationLockTableName) + " VALUES ('" + name + "', 'x', TIMESTAMP '2020-01-01 00:00:00')";
            DbException duplicate = await Assert.ThrowsAnyAsync<DbException>(() => ScalarAsync(contender, new SqlStatement(insertHeld)));
            Assert.True(dialect.IsMigrationLockContention(duplicate));
            await ScalarAsync(holder, dialect.ReleaseMigrationLockSql(name)!);
            DbException syntax = await Assert.ThrowsAnyAsync<DbException>(() => ScalarAsync(contender, new SqlStatement("SELEC 1")));
            Assert.False(dialect.IsMigrationLockContention(syntax));
            Assert.False(dialect.IsMigrationLockContention(new InvalidOperationException("Conflict")));
        }

        /// <summary>
        /// Schema sync adds a NOT NULL column to, and drops a column from, a table with indexes (which DuckDB only allows
        /// without indexes) by re-creating the table's indexes around the change.
        /// </summary>
        [Fact]
        public async Task SchemaSync_AltersIndexedTable()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await ExecuteAsync(factory, RelTestHelpers.DropTableSql(_Provider.Dialect, "duck_indexed"));
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);
            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
            migrator.SyncSchema(new[] { EntityMetadata.For<DuckIndexedSlim>() });
            await ExecuteAsync(factory, "INSERT INTO duck_indexed (name, code) VALUES ('a', 'x')");

            SchemaSyncResult widened = await migrator.SyncSchemaAsync(new[] { EntityMetadata.For<DuckIndexedFull>() });
            Assert.Equal(2, widened.AppliedOperations.Count);
            TableSchema wide = reader.ReadTable("duck_indexed")!;
            Assert.False(wide.FindColumn("qty")!.IsNullable);
            Assert.NotNull(wide.FindIndex("idx_duck_indexed_name"));
            Assert.NotNull(wide.FindIndex("idx_duck_indexed_code"));
            Assert.True(migrator.DiffSchema(new[] { EntityMetadata.For<DuckIndexedFull>() }).IsEmpty);

            SchemaSyncResult trimmed = migrator.SyncSchema(new[] { EntityMetadata.For<DuckIndexedSlim>() }, new SchemaSyncOptions { AllowDestructive = true });
            Assert.Equal(2, trimmed.AppliedOperations.Count);
            TableSchema slim = reader.ReadTable("duck_indexed")!;
            Assert.Null(slim.FindColumn("legacy"));
            Assert.Null(slim.FindColumn("qty"));
            Assert.NotNull(slim.FindIndex("idx_duck_indexed_name"));
            Assert.NotNull(slim.FindIndex("idx_duck_indexed_code"));
            Assert.True(migrator.DiffSchema(new[] { EntityMetadata.For<DuckIndexedSlim>() }).IsEmpty);
            await ExecuteAsync(factory, RelTestHelpers.DropTableSql(_Provider.Dialect, "duck_indexed"));
        }

        #endregion

        #region Private-Methods

        private static DuckTypesItem NewItem(string name)
        {
            return new DuckTypesItem
            {
                Name = name,
                Tiny = 7,
                SignedTiny = -7,
                Small = 700,
                Medium = 70000,
                Large = 7000000000UL,
                Uid = Guid.NewGuid(),
                At = new DateTimeOffset(2026, 10, 7, 12, 30, 15, TimeSpan.FromHours(2)),
                Stamp = new DateTime(2026, 10, 7, 8, 15, 30, 123),
                Span = TimeSpan.FromHours(30).Add(TimeSpan.FromMilliseconds(250)),
                Day = new DateOnly(2026, 10, 7),
                Clock = new TimeOnly(23, 59, 58),
                Price = 1234.5678m,
                Data = new byte[] { 0, 1, 2, 254, 255 },
                Tags = new[] { "a", "b" }
            };
        }

        private static async Task<object?> ScalarAsync(DbConnection connection, SqlStatement statement)
        {
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = statement.Sql;
            foreach (SqlParameterValue value in statement.Parameters)
            {
                DbParameter parameter = command.CreateParameter();
                parameter.ParameterName = value.Name;
                parameter.Value = value.Value ?? DBNull.Value;
                DuckDbDialect.Default.ConfigureParameter(parameter, value);
                command.Parameters.Add(parameter);
            }

            object? result = await command.ExecuteScalarAsync();
            return result == DBNull.Value ? null : result;
        }

        private static async Task ExecuteAsync(IConnectionFactory factory, string sql)
        {
            await using DbConnection connection = await factory.OpenConnectionAsync();
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        #endregion
    }
}
