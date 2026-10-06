namespace Test.Aot
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using Durable;
    using Durable.Sql;
    using Durable.Sqlite;

    /// <summary>
    /// SQLite end-to-end checks: schema creation, the shared repository scenario, raw SQL, migrations and schema sync,
    /// and the 10k-row materialization timing.
    /// </summary>
    internal static class SqliteScenario
    {
        public static async Task RunAsync(CheckRunner runner, int rowCount, int iterations)
        {
            string path = Path.Combine(Path.GetTempPath(), "durable-aot-" + Guid.NewGuid().ToString("N") + ".db");
            string connectionString = "Data Source=" + path;
            try
            {
                using (SqliteConnectionFactory factory = new SqliteConnectionFactory(connectionString))
                {
                    SqlRepositoryOptions options = new SqlRepositoryOptions
                    {
                        DataTypeConverter = new SqliteDataTypeConverter(DurableJson.CreateOptions(AotJsonContext.Default))
                    };

                    using SqliteRepository<Author> authors = new SqliteRepository<Author>(factory, options);
                    using SqliteRepository<Book> books = new SqliteRepository<Book>(factory, options);
                    using SqliteRepository<Review> reviews = new SqliteRepository<Review>(factory, options);
                    using SqliteRepository<Category> categories = new SqliteRepository<Category>(factory, options);
                    using SqliteRepository<AuthorCategory> links = new SqliteRepository<AuthorCategory>(factory, options);
                    using SqliteRepository<Shelf> shelves = new SqliteRepository<Shelf>(factory, options);
                    using SqliteRepository<Reading> readings = new SqliteRepository<Reading>(factory, options);

                    runner.Check("[sqlite] Create schema (InitializeTable)", () =>
                    {
                        authors.InitializeTable(typeof(Author));
                        books.InitializeTable(typeof(Book));
                        reviews.InitializeTable(typeof(Review));
                        categories.InitializeTable(typeof(Category));
                        links.InitializeTable(typeof(AuthorCategory));
                        shelves.InitializeTable(typeof(Shelf));
                        readings.InitializeTable(typeof(Reading));
                        shelves.ExecuteSql("CREATE TABLE shelf_items (id INTEGER PRIMARY KEY AUTOINCREMENT, shelf_id INTEGER NOT NULL, label TEXT NOT NULL)");
                        return authors.ExecuteScalar<long>("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('authors','books','reviews','categories','author_categories','shelves','readings')") == 7;
                    });
                    runner.Check("[sqlite] Nullable reference annotations honored (NOT NULL name, NULL email)", () =>
                    {
                        string ddl = authors.ExecuteScalar<string>("SELECT sql FROM sqlite_master WHERE name = 'authors'") ?? string.Empty;
                        EntityMetadata metadata = EntityMetadata.For<Author>();
                        return !metadata.FindColumnByProperty("Name")!.IsNullable && metadata.FindColumnByProperty("Email")!.IsNullable && ddl.Length > 0;
                    });

                    await RepositoryScenario.RunAsync(runner, new RepositorySet("sqlite", authors, books, reviews, categories, links, shelves)).ConfigureAwait(false);

                    runner.Check("[sqlite] FromSql<TResult> to DTO", () =>
                    {
                        List<AuthorSummary> rows = authors.FromSql<AuthorSummary>("SELECT name AS Name, rating AS Rating FROM authors WHERE rating >= @p0 ORDER BY name", null, 5).ToList();
                        return rows.Count == 2 && rows[0].Name == "Ada Lovelace";
                    });
                    runner.Check("[sqlite] JSON stored as camelCase text", () =>
                    {
                        string? json = books.ExecuteScalar<string>("SELECT details FROM books WHERE title = @p0", null, "Notes on the Analytical Engine");
                        return json != null && json.Contains("\"pages\":64", StringComparison.Ordinal);
                    });

                    RunMigrations(runner, factory);
                    await RunMigrationsAsync(runner, factory).ConfigureAwait(false);
                    await TimeReadsAsync(runner, factory, readings, rowCount, iterations).ConfigureAwait(false);
                }
            }
            finally
            {
                SqliteConnection.ClearAllPools();
                TryDelete(path);
                TryDelete(path + "-wal");
                TryDelete(path + "-shm");
            }
        }

        private static void RunMigrations(CheckRunner runner, SqliteConnectionFactory factory)
        {
            SqlMigrator migrator = new SqlMigrator(factory, SqliteDialect.Default).AddMigration(new CreateLedgerMigration());
            runner.Check("[sqlite] Migration applies once", () =>
            {
                MigrationRunResult first = migrator.Migrate();
                MigrationRunResult second = migrator.Migrate();
                return first.Applied.Count == 1 && second.Applied.Count == 0 && migrator.GetAppliedMigrations().Count == 1;
            });
            runner.Check("[sqlite] Migration rollback (Down)", () =>
            {
                MigrationRunResult result = migrator.RollbackTo(null);
                return result.Reverted.Count == 1 && migrator.GetPendingMigrations().Count == 1 && migrator.Migrate().Applied.Count == 1;
            });
            runner.Check("[sqlite] Schema sync with EntityMetadata (AOT-safe overload)", () =>
            {
                SchemaSyncResult result = migrator.SyncSchema(new[] { EntityMetadata.For<Author>(), EntityMetadata.For<Book>() });
                SchemaDiff diff = migrator.DiffSchema(new[] { EntityMetadata.For<Author>(), EntityMetadata.For<Book>() });
                return result != null && diff.AdditiveOperations.Count == 0;
            });
        }

        private static async Task RunMigrationsAsync(CheckRunner runner, SqliteConnectionFactory factory)
        {
            SqlMigrator migrator = new SqlMigrator(factory, SqliteDialect.Default, new SqlMigratorOptions { HistoryTableName = "aot_history_async" });
            migrator.AddMigration(new CreateLedgerMigration());
            await runner.CheckAsync("[sqlite] Script generation (async)", async () =>
            {
                string script = await migrator.GenerateScriptAsync().ConfigureAwait(false);
                return script.Contains("CREATE TABLE ledger", StringComparison.Ordinal);
            }).ConfigureAwait(false);
        }

        private static async Task TimeReadsAsync(CheckRunner runner, SqliteConnectionFactory factory, SqliteRepository<Reading> readings, int rowCount, int iterations)
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

            using (ITransaction tx = readings.BeginTransaction())
            {
                readings.BulkInsert(rows, tx);
                tx.Commit();
            }

            runner.Check("[sqlite] " + rowCount + " rows inserted", () => readings.Count() == rowCount);

            List<Reading> warm = readings.ReadAll().ToList();
            runner.Check("[sqlite] " + rowCount + " rows materialized correctly", () =>
            {
                Reading r = warm.First(x => x.Sensor == "sensor-7" && x.Value == 3.5);
                return warm.Count == rowCount && r.Amount == 8.75m && r.Taken == start.AddSeconds(7) && !r.Active && r.Status == AuthorStatus.Retired && r.Note == "note 7"
                    && warm.Count(x => x.Note == null) == rowCount / 4;
            });

            double durable = Measure(iterations, () => readings.ReadAll().Count());
            Stopwatch asyncWatch = new Stopwatch();
            List<double> asyncTimes = new List<double>();
            for (int i = 0; i < iterations; i++)
            {
                asyncWatch.Restart();
                int count = 0;
                await foreach (Reading r in readings.ReadAllAsync().ConfigureAwait(false)) count++;
                asyncWatch.Stop();
                asyncTimes.Add(asyncWatch.Elapsed.TotalMilliseconds);
            }

            asyncTimes.Sort();
            double durableAsync = asyncTimes[asyncTimes.Count / 2];
            double ado = Measure(iterations, () => ReadRaw(factory));

            string mode = RuntimeMode.Describe();
            Console.WriteLine();
            Console.WriteLine("TIMING [" + mode + "] read " + rowCount + " rows (median of " + iterations + "):");
            Console.WriteLine("TIMING   Durable ReadAll           " + durable.ToString("F2") + " ms");
            Console.WriteLine("TIMING   Durable ReadAllAsync      " + durableAsync.ToString("F2") + " ms");
            Console.WriteLine("TIMING   ADO.NET hand-written      " + ado.ToString("F2") + " ms");
            Console.WriteLine("TIMING   Durable / ADO.NET         " + (durable / ado).ToString("F2") + "x");
            Console.WriteLine();
        }

        private static int ReadRaw(SqliteConnectionFactory factory)
        {
            using SqliteConnection connection = new SqliteConnection(factory.ConnectionString);
            connection.Open();
            using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT id, sensor, value, amount, taken, active, status, note, external_id FROM readings";
            using SqliteDataReader reader = command.ExecuteReader();
            List<Reading> list = new List<Reading>();
            while (reader.Read())
            {
                list.Add(new Reading
                {
                    Id = reader.GetInt32(0),
                    Sensor = reader.GetString(1),
                    Value = reader.GetDouble(2),
                    Amount = reader.GetDecimal(3),
                    Taken = reader.GetDateTime(4),
                    Active = reader.GetBoolean(5),
                    Status = Enum.Parse<AuthorStatus>(reader.GetString(6)),
                    Note = reader.IsDBNull(7) ? null : reader.GetString(7),
                    ExternalId = reader.GetGuid(8)
                });
            }

            return list.Count;
        }

        private static double Measure(int iterations, Func<int> action)
        {
            action();
            List<double> times = new List<double>();
            Stopwatch watch = new Stopwatch();
            for (int i = 0; i < iterations; i++)
            {
                watch.Restart();
                action();
                watch.Stop();
                times.Add(watch.Elapsed.TotalMilliseconds);
            }

            times.Sort();
            return times[times.Count / 2];
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path)) File.Delete(path);
            }
            catch (IOException)
            {
            }
        }
    }
}
