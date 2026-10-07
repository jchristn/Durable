namespace Test.Aot
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.DuckDb;
    using Durable.Sql;

    /// <summary>
    /// DuckDB end-to-end checks on a private in-memory database: schema creation (sequence-backed keys), the shared
    /// repository scenario, raw SQL, JSON, Appender bulk insert, HUGEINT aggregates, migrations and schema sync.
    /// </summary>
    internal static class DuckDbScenario
    {
        public static async Task RunAsync(CheckRunner runner, int rowCount)
        {
            using DuckDbConnectionFactory factory = new DuckDbConnectionFactory(DuckDbRepositorySettings.ForInMemory());
            SqlRepositoryOptions options = new SqlRepositoryOptions
            {
                DataTypeConverter = new DuckDbDataTypeConverter(DurableJson.CreateOptions(AotJsonContext.Default))
            };

            using DuckDbRepository<Author> authors = new DuckDbRepository<Author>(factory, options);
            using DuckDbRepository<Book> books = new DuckDbRepository<Book>(factory, options);
            using DuckDbRepository<Review> reviews = new DuckDbRepository<Review>(factory, options);
            using DuckDbRepository<Category> categories = new DuckDbRepository<Category>(factory, options);
            using DuckDbRepository<AuthorCategory> links = new DuckDbRepository<AuthorCategory>(factory, options);
            using DuckDbRepository<Shelf> shelves = new DuckDbRepository<Shelf>(factory, options);
            using DuckDbRepository<Reading> readings = new DuckDbRepository<Reading>(factory, options);

            runner.Check("[duckdb] Create schema (InitializeTable)", () =>
            {
                authors.InitializeTable(typeof(Author));
                books.InitializeTable(typeof(Book));
                reviews.InitializeTable(typeof(Review));
                categories.InitializeTable(typeof(Category));
                links.InitializeTable(typeof(AuthorCategory));
                shelves.InitializeTable(typeof(Shelf));
                readings.InitializeTable(typeof(Reading));
                shelves.ExecuteSqlRaw("CREATE SEQUENCE shelf_items_id_seq; CREATE TABLE shelf_items (id INTEGER PRIMARY KEY DEFAULT nextval('shelf_items_id_seq'), shelf_id INTEGER NOT NULL, label VARCHAR NOT NULL)");
                return authors.ExecuteScalarRaw<long>("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('authors','books','reviews','categories','author_categories','shelves','readings')") == 7;
            });

            await RepositoryScenario.RunAsync(runner, new RepositorySet("duckdb", authors, books, reviews, categories, links, shelves)).ConfigureAwait(false);

            runner.Check("[duckdb] FromSql<TResult> to DTO", () =>
            {
                List<AuthorSummary> rows = authors.FromSqlRaw<AuthorSummary>("SELECT name AS Name, rating AS Rating FROM authors WHERE rating >= {0} ORDER BY name", new object?[] { 5 }).ToList();
                return rows.Count == 2 && rows[0].Name == "Ada Lovelace";
            });
            runner.Check("[duckdb] JSON stored as camelCase JSON", () =>
            {
                string? json = books.ExecuteScalarRaw<string>("SELECT CAST(details AS VARCHAR) FROM books WHERE title = {0}", new object?[] { "Notes on the Analytical Engine" });
                return json != null && json.Contains("\"pages\":64", StringComparison.Ordinal);
            });
            runner.Check("[duckdb] Savepoints report NotSupportedException", () =>
            {
                using ISqlTransaction transaction = authors.BeginTransaction();
                try
                {
                    transaction.CreateSavepoint("sp");
                    return false;
                }
                catch (NotSupportedException)
                {
                    transaction.Rollback();
                    return true;
                }
            });

            RunMigrations(runner, factory);
            await BulkAsync(runner, readings, rowCount).ConfigureAwait(false);
        }

        private static void RunMigrations(CheckRunner runner, DuckDbConnectionFactory factory)
        {
            SqlMigrator migrator = new SqlMigrator(factory, DuckDbDialect.Default).AddMigration(new CreateLedgerMigration());
            runner.Check("[duckdb] Migration applies once (row lock)", () =>
            {
                MigrationRunResult first = migrator.Migrate();
                MigrationRunResult second = migrator.Migrate();
                return first.Applied.Count == 1 && second.Applied.Count == 0 && migrator.GetAppliedMigrations().Count == 1;
            });
            runner.Check("[duckdb] Migration rollback (Down)", () =>
            {
                MigrationRunResult result = migrator.RollbackTo(null);
                return result.Reverted.Count == 1 && migrator.GetPendingMigrations().Count == 1 && migrator.Migrate().Applied.Count == 1;
            });
            runner.Check("[duckdb] Schema sync with EntityMetadata (AOT-safe overload)", () =>
            {
                SchemaSyncResult result = migrator.SyncSchema(new[] { EntityMetadata.For<Author>(), EntityMetadata.For<Book>() });
                SchemaDiff diff = migrator.DiffSchema(new[] { EntityMetadata.For<Author>(), EntityMetadata.For<Book>() });
                return result != null && diff.AdditiveOperations.Count == 0;
            });
        }

        private static async Task BulkAsync(CheckRunner runner, DuckDbRepository<Reading> readings, int rowCount)
        {
            DateTime start = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            List<Reading> rows = new List<Reading>(rowCount);
            for (int i = 0; i < rowCount; i++)
            {
                rows.Add(new Reading
                {
                    Sensor = "sensor-" + (i % 50),
                    Value = i * 0.5,
                    Amount = i * 1.25m,
                    Taken = start.AddSeconds(i),
                    Active = i % 2 == 0,
                    Status = (AuthorStatus)(i % 3),
                    Note = i % 4 == 0 ? null : "note " + i,
                    ExternalId = Guid.NewGuid()
                });
            }

            await runner.CheckAsync("[duckdb] " + rowCount + " rows bulk inserted (Appender)", async () =>
            {
                long inserted = await readings.BulkInsertAsync(rows).ConfigureAwait(false);
                return inserted == rowCount && await readings.CountAsync().ConfigureAwait(false) == rowCount;
            }).ConfigureAwait(false);

            runner.Check("[duckdb] " + rowCount + " rows materialized correctly", () =>
            {
                List<Reading> all = readings.ReadAll().ToList();
                Reading r = all.First(x => x.Sensor == "sensor-7" && x.Value == 3.5);
                return all.Count == rowCount && r.Amount == 8.75m && r.Taken == start.AddSeconds(7) && !r.Active && r.Status == (AuthorStatus)1 && r.Note == "note 7";
            });

            await runner.CheckAsync("[duckdb] SUM over integers (HUGEINT) and decimals", async () =>
            {
                decimal ids = await readings.Query().SumAsync(x => x.Id).ConfigureAwait(false);
                decimal amounts = await readings.Query().SumAsync(x => x.Amount).ConfigureAwait(false);
                long expected = (long)rowCount * (rowCount - 1) / 2;
                return ids == expected + rowCount && amounts == expected * 1.25m;
            }).ConfigureAwait(false);
        }
    }
}
