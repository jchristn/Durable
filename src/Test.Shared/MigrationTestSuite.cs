namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Coverage for lightweight migrations: live schema introspection, schema diffing, additive and destructive schema
    /// synchronization, versioned migrations (history, pending detection, failure handling, concurrent migrators,
    /// rollback) and script generation. Executed identically across all database providers; every test uses its own
    /// tables and history table.
    /// </summary>
    public class MigrationTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="MigrationTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public MigrationTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// A table created by schema sync reads back with the mapped columns, nullability, key, lengths and indexes, and
        /// diffs as empty against its own mapping (the type normalization round-trips); syncing again does nothing.
        /// </summary>
        [Fact]
        public async Task SchemaReader_RoundTripsCreatedTable()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_all_types");
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);
            Type[] types = new[] { typeof(MigAllTypes) };

            SchemaSyncResult created = await migrator.SyncSchemaAsync(types);
            Assert.Contains(created.AppliedOperations, o => o.Kind == MigrationOperationKind.CreateTable && o.TableName == "mig_all_types");
            Assert.Equal(2, created.AppliedOperations.Count(o => o.Kind == MigrationOperationKind.CreateIndex));

            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
            Assert.True(await reader.TableExistsAsync("mig_all_types"));
            Assert.False(reader.TableExists("mig_no_such_table"));
            Assert.Null(await reader.ReadTableAsync("mig_no_such_table"));

            TableSchema? table = reader.ReadTable("mig_all_types");
            Assert.NotNull(table);
            Assert.Equal(16, table!.Columns.Count);
            Assert.Equal(new[] { "id" }, table.PrimaryKeyColumns);
            Assert.Equal("id", table.Columns[0].Name, StringComparer.OrdinalIgnoreCase);
            Assert.False(table.FindColumn("name")!.IsNullable);
            Assert.True(table.FindColumn("code")!.IsNullable);
            Assert.True(table.FindColumn("maybe")!.IsNullable);
            Assert.False(table.FindColumn("count")!.IsNullable);
            Assert.False(table.FindColumn("uid")!.IsNullable);
            if (_Provider.DatabaseType != TestDatabaseType.Sqlite)
            {
                Assert.Equal(50, table.FindColumn("name")!.MaxLength);
                Assert.Equal(20, table.FindColumn("code")!.MaxLength);
            }

            IndexSchema? single = table.FindIndex("idx_mig_all_types_name");
            Assert.NotNull(single);
            Assert.Equal(new[] { "name" }, single!.Columns, StringComparer.OrdinalIgnoreCase);
            Assert.False(single.IsUnique);
            IndexSchema? composite = table.FindIndex("idx_mig_all_code_flag");
            Assert.NotNull(composite);
            Assert.Equal(new[] { "code", "flag" }, composite!.Columns, StringComparer.OrdinalIgnoreCase);
            Assert.True(composite.IsUnique);

            SchemaDiff diff = await migrator.DiffSchemaAsync(types);
            Assert.True(diff.IsEmpty, "Expected no differences but got: " + string.Join(" | ", diff.Operations.Select(o => o.ToString()).Concat(diff.Differences.Select(d => d.ToString()))));

            SchemaSyncResult again = migrator.SyncSchema(types);
            Assert.Empty(again.AppliedOperations);
            Assert.True(again.IsInSync);

            Console.WriteLine("     Round-tripped " + table.Columns.Count + " columns and " + table.Indexes.Count + " indexes");
        }

        /// <summary>
        /// Missing tables, columns and indexes are detected; additive sync fills existing rows with derived defaults,
        /// reports NOT NULL columns without a default, and is idempotent.
        /// </summary>
        [Fact]
        public async Task SchemaSync_AddsMissingColumnsAndIndexes()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_widgets");
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);

            SchemaDiff missing = migrator.DiffSchema(new[] { typeof(MigWidgetV1) });
            Assert.Single(missing.Operations);
            Assert.Equal(MigrationOperationKind.CreateTable, missing.Operations[0].Kind);
            Assert.Empty(missing.Differences);

            migrator.SyncSchema(new[] { typeof(MigWidgetV1) });
            await ExecuteAsync(factory, "INSERT INTO " + Q("mig_widgets") + " (" + Q("name") + ") VALUES ('existing')");

            SchemaDiff diff = migrator.DiffSchema(new[] { typeof(MigWidgetV2) });
            Assert.Empty(diff.DestructiveOperations);
            Assert.Contains(diff.Operations, o => o.Kind == MigrationOperationKind.AddColumn && o.ColumnName == "email");
            Assert.Contains(diff.Operations, o => o.Kind == MigrationOperationKind.AddColumn && o.ColumnName == "quantity" && o.Description.Contains("DEFAULT 0"));
            Assert.Contains(diff.Operations, o => o.Kind == MigrationOperationKind.AddColumn && o.ColumnName == "is_active");
            Assert.Contains(diff.Operations, o => o.Kind == MigrationOperationKind.CreateIndex && o.IndexName == "idx_mig_widgets_email");
            Assert.DoesNotContain(diff.Operations, o => o.ColumnName == "label");
            SchemaDifference label = Assert.Single(diff.Differences);
            Assert.Equal(SchemaDifferenceKind.NotNullColumnWithoutDefault, label.Kind);
            Assert.Equal("label", label.ColumnName);

            SchemaSyncResult result = await migrator.SyncSchemaAsync(new[] { typeof(MigWidgetV2) });
            Assert.Equal(4, result.AppliedOperations.Count);
            Assert.Single(result.Differences);
            Assert.False(result.IsInSync);
            Assert.Equal(0L, ToInt64(await ScalarAsync(factory, "SELECT " + Q("quantity") + " FROM " + Q("mig_widgets"))));
            Assert.Equal(1L, ToInt64(await ScalarAsync(factory, "SELECT " + Q("is_active") + " FROM " + Q("mig_widgets"))));
            Assert.Null(await ScalarAsync(factory, "SELECT " + Q("email") + " FROM " + Q("mig_widgets")));

            SchemaSyncResult idempotent = migrator.SyncSchema(new[] { typeof(MigWidgetV2) });
            Assert.Empty(idempotent.AppliedOperations);
            Assert.Single(idempotent.Differences);

            SchemaSyncResult nullable = migrator.SyncSchema(new[] { typeof(MigWidgetV2) }, new SchemaSyncOptions { AddUnresolvableNotNullColumnsAsNullable = true });
            MigrationOperation added = Assert.Single(nullable.AppliedOperations);
            Assert.Equal("label", added.ColumnName);
            Assert.NotNull(added.Warning);

            SchemaDiff after = migrator.DiffSchema(new[] { typeof(MigWidgetV2) });
            Assert.Empty(after.Operations);
            SchemaDifference nullability = Assert.Single(after.Differences);
            Assert.Equal(SchemaDifferenceKind.NullabilityMismatch, nullability.Kind);
            Assert.Equal("label", nullability.ColumnName);

            SchemaSyncResult reverted = migrator.SyncSchema(new[] { typeof(MigWidgetV1) }, new SchemaSyncOptions { AllowDestructive = true });
            Assert.Equal(5, reverted.AppliedOperations.Count);
            Assert.All(reverted.AppliedOperations, o => Assert.True(o.IsDestructive));
            Assert.True(migrator.DiffSchema(new[] { typeof(MigWidgetV1) }).IsEmpty);
            Assert.Equal("existing", Convert.ToString(await ScalarAsync(factory, "SELECT " + Q("name") + " FROM " + Q("mig_widgets")), CultureInfo.InvariantCulture));

            Console.WriteLine("     Applied " + result.AppliedOperations.Count + " additive operations; label reported then added as nullable; reverted with " + reverted.AppliedOperations.Count + " destructive operations");
        }

        /// <summary>
        /// Type, length and nullability differences are reported (never applied).
        /// </summary>
        [Fact]
        public async Task SchemaDiff_ReportsTypeLengthAndNullabilityDifferences()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_typed");
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);
            migrator.SyncSchema(new[] { typeof(MigTypedV1) });
            Assert.True(migrator.DiffSchema(new[] { typeof(MigTypedV1) }).IsEmpty);

            SchemaDiff diff = await migrator.DiffSchemaAsync(new[] { typeof(MigTypedV2) });
            Assert.Empty(diff.Operations);
            Assert.Contains(diff.Differences, d => d.Kind == SchemaDifferenceKind.TypeMismatch && d.ColumnName == "count");
            Assert.Contains(diff.Differences, d => d.Kind == SchemaDifferenceKind.NullabilityMismatch && d.ColumnName == "note" && d.Expected == "NULL" && d.Actual == "NOT NULL");
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite)
                Assert.DoesNotContain(diff.Differences, d => d.ColumnName == "name");
            else
                Assert.Contains(diff.Differences, d => d.Kind == SchemaDifferenceKind.MaxLengthMismatch && d.ColumnName == "name");

            SchemaSyncResult result = migrator.SyncSchema(new[] { typeof(MigTypedV2) });
            Assert.Empty(result.AppliedOperations);
            Assert.Equal(diff.Differences.Count, result.Differences.Count);
            Assert.Contains("MANUAL STEP REQUIRED", diff.ToScript());

            Console.WriteLine("     Reported " + diff.Differences.Count + " differences: " + string.Join("; ", diff.Differences.Select(d => d.Kind + " " + d.ColumnName)));
        }

        /// <summary>
        /// Dropping unmapped columns and indexes is destructive: skipped by default, applied only when allowed.
        /// </summary>
        [Fact]
        public async Task SchemaSync_DestructiveOperationsRequireOptIn()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_trim");
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);
            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
            migrator.SyncSchema(new[] { typeof(MigTrimFull) });

            SchemaDiff diff = migrator.DiffSchema(new[] { typeof(MigTrimSlim) });
            Assert.Empty(diff.AdditiveOperations);
            Assert.Equal(2, diff.DestructiveOperations.Count);
            Assert.Contains(diff.DestructiveOperations, o => o.Kind == MigrationOperationKind.DropIndex && o.IndexName == "idx_mig_trim_extra");
            Assert.Contains(diff.DestructiveOperations, o => o.Kind == MigrationOperationKind.DropColumn && o.ColumnName == "extra");
            Assert.Equal(MigrationOperationKind.DropIndex, diff.Operations[0].Kind);

            string script = migrator.GenerateSyncScript(new[] { typeof(MigTrimSlim) });
            Assert.Contains("SKIPPED (destructive)", script);
            Assert.DoesNotContain("DROP COLUMN", script);
            Assert.Contains("DROP COLUMN", migrator.GenerateSyncScript(new[] { typeof(MigTrimSlim) }, new SchemaSyncOptions { AllowDestructive = true }));

            SchemaSyncResult skipped = await migrator.SyncSchemaAsync(new[] { typeof(MigTrimSlim) });
            Assert.Empty(skipped.AppliedOperations);
            Assert.Equal(2, skipped.SkippedOperations.Count);
            TableSchema? unchanged = reader.ReadTable("mig_trim");
            Assert.NotNull(unchanged!.FindColumn("extra"));
            Assert.NotNull(unchanged.FindIndex("idx_mig_trim_extra"));

            SchemaSyncResult applied = await migrator.SyncSchemaAsync(new[] { typeof(MigTrimSlim) }, new SchemaSyncOptions { AllowDestructive = true });
            Assert.Equal(2, applied.AppliedOperations.Count);
            Assert.True(applied.IsInSync);
            TableSchema? trimmed = reader.ReadTable("mig_trim");
            Assert.Null(trimmed!.FindColumn("extra"));
            Assert.Null(trimmed.FindIndex("idx_mig_trim_extra"));
            Assert.True(migrator.DiffSchema(new[] { typeof(MigTrimSlim) }).IsEmpty);

            Console.WriteLine("     Destructive operations skipped by default and applied when allowed");
        }

        /// <summary>
        /// Versioned migrations are applied in order, recorded once, detected as pending, and not re-applied.
        /// </summary>
        [Fact]
        public async Task Migrate_AppliesPendingMigrationsOnce()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_hist_basic", "mig_notes");
            SqlMigratorOptions options = new SqlMigratorOptions { HistoryTableName = "mig_hist_basic" };
            List<Migration> migrations = new List<Migration>
            {
                new DelegateMigration("20261005_0001_CreateNotes", "Create notes", ctx => ctx.EnsureSchema(typeof(MigNote))),
                new DelegateMigration("20261005_0002_SeedNote", "Seed a note", ctx =>
                    ctx.ExecuteSql("INSERT INTO " + ctx.Dialect.QuoteIdentifier("mig_notes") + " (" + ctx.Dialect.QuoteIdentifier("body") + ") VALUES (@p0)", "hello"))
            };

            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect, migrations, options);
            Assert.Empty(migrator.GetAppliedMigrations());
            Assert.Equal(new[] { "20261005_0001_CreateNotes", "20261005_0002_SeedNote" }, migrator.GetPendingMigrations().Select(m => m.Id));

            MigrationRunResult first = migrator.Migrate();
            Assert.Equal(new[] { "20261005_0001_CreateNotes", "20261005_0002_SeedNote" }, first.Applied.Select(a => a.Id));
            Assert.Empty(first.Skipped);

            List<AppliedMigration> history = migrator.GetAppliedMigrations();
            Assert.Equal(2, history.Count);
            Assert.Equal("Create notes", history[0].Description);
            Assert.True(history[0].AppliedUtc > DateTime.UtcNow.AddDays(-2) && history[0].AppliedUtc < DateTime.UtcNow.AddDays(2));
            Assert.Empty(migrator.GetPendingMigrations());

            MigrationRunResult second = migrator.Migrate();
            Assert.Empty(second.Applied);
            Assert.Equal(1L, await CountAsync(factory, "mig_notes"));

            migrations.Add(new DelegateMigration("20261005_0003_SeedAnother", "Seed another", ctx =>
            {
                long count = ToInt64(ctx.ExecuteScalar("SELECT COUNT(*) FROM " + ctx.Dialect.QuoteIdentifier("mig_notes")));
                ctx.ExecuteSql("INSERT INTO " + ctx.Dialect.QuoteIdentifier("mig_notes") + " (" + ctx.Dialect.QuoteIdentifier("body") + ") VALUES (@p0)", "after " + count);
            }));
            SqlMigrator extended = new SqlMigrator(factory, _Provider.Dialect, migrations, options);
            Assert.Equal("20261005_0003_SeedAnother", Assert.Single(extended.GetPendingMigrations()).Id);
            Assert.Single(extended.Migrate().Applied);
            Assert.Equal(2L, await CountAsync(factory, "mig_notes"));
            Assert.Equal("after 1", Convert.ToString(await ScalarAsync(factory, "SELECT " + Q("body") + " FROM " + Q("mig_notes") + " WHERE " + Q("id") + " = 2"), CultureInfo.InvariantCulture));

            Console.WriteLine("     Applied 3 migrations across runs; history has " + extended.GetAppliedMigrations().Count + " rows");
        }

        /// <summary>
        /// The async API orders migrations by Id regardless of registration order, and a migration can run repository
        /// operations inside its transaction through <see cref="MigrationContext.Transaction"/>.
        /// </summary>
        [Fact]
        public async Task MigrateAsync_OrdersByIdAndSupportsRepositoriesInTransaction()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_hist_async", "mig_notes");
            ISqlRepository<MigNote> repository = _Provider.CreateRepository<MigNote>(factory);
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect, new SqlMigratorOptions { HistoryTableName = "mig_hist_async" });
            migrator.AddMigration(new DelegateMigration("20261006_0002_Seed", "Seed through repository", ctx =>
                repository.Create(new MigNote { Body = "from repository" }, ctx.Transaction)));
            migrator.AddMigration(new DelegateMigration("20261006_0001_Create", "Create", ctx => ctx.EnsureSchema(typeof(MigNote))));

            List<Migration> pending = await migrator.GetPendingMigrationsAsync();
            Assert.Equal(new[] { "20261006_0001_Create", "20261006_0002_Seed" }, pending.Select(m => m.Id));

            MigrationRunResult result = await migrator.MigrateAsync();
            Assert.Equal(2, result.Applied.Count);
            Assert.Equal("20261006_0001_Create", result.Applied[0].Id);
            Assert.Empty(await migrator.GetPendingMigrationsAsync());
            Assert.Equal(2, (await migrator.GetAppliedMigrationsAsync()).Count);
            MigNote? note = await repository.ReadFirstAsync(n => n.Body == "from repository");
            Assert.NotNull(note);
            Assert.Throws<ArgumentException>(() => migrator.AddMigration(new DelegateMigration("20261006_0001_Create", null, ctx => { })));

            Console.WriteLine("     Applied 2 migrations asynchronously in Id order");
        }

        /// <summary>
        /// Concrete <see cref="Migration"/> classes are discovered from an assembly.
        /// </summary>
        [Fact]
        public async Task AddMigrationsFromAssembly_DiscoversConcreteMigrations()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_hist_discovered", "mig_discovered");
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect, new SqlMigratorOptions { HistoryTableName = "mig_hist_discovered" })
                .AddMigrationsFromAssembly(typeof(MigDiscoveredFirst).Assembly, t => t.Name.StartsWith("MigDiscovered", StringComparison.Ordinal));
            Assert.Equal(new[] { "20261005_0001_DiscoveredFirst", "20261005_0002_DiscoveredSecond" }, migrator.Migrations.Select(m => m.Id));

            MigrationRunResult result = migrator.Migrate();
            Assert.Equal(2, result.Applied.Count);
            Assert.Equal(1L, await CountAsync(factory, "mig_discovered"));
            Assert.Equal("Create discovered items table", migrator.GetAppliedMigrations()[0].Description);
            Assert.Equal("MigDiscoveredSecond", migrator.GetAppliedMigrations()[1].Description);

            Console.WriteLine("     Discovered and applied 2 migrations");
        }

        /// <summary>
        /// A failing migration is not recorded; on databases with transactional DDL its changes are rolled back,
        /// otherwise the exception reports possible partial application. Later migrations do not run.
        /// </summary>
        [Fact]
        public async Task Migrate_FailingMigrationIsNotRecorded()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_hist_fail", "mig_fail_items", "mig_fail_partial");
            ISqlDialect dialect = _Provider.Dialect;
            List<Migration> migrations = new List<Migration>
            {
                new DelegateMigration("20261005_0001_Ok", "Works", ctx => ctx.EnsureSchema(typeof(MigFailItem))),
                new DelegateMigration("20261005_0002_Broken", "Fails halfway", ctx =>
                {
                    ctx.EnsureSchema(typeof(MigFailPartial));
                    ctx.ExecuteSql("INSERT INTO " + dialect.QuoteIdentifier("mig_fail_items") + " (" + dialect.QuoteIdentifier("name") + ") VALUES (@p0)", "partial");
                    ctx.ExecuteSql("INSERT INTO " + dialect.QuoteIdentifier("mig_no_such_table_for_failure") + " (x) VALUES (1)");
                }),
                new DelegateMigration("20261005_0003_Never", "Never runs", ctx => ctx.ExecuteSql("INSERT INTO " + dialect.QuoteIdentifier("mig_fail_items") + " (" + dialect.QuoteIdentifier("name") + ") VALUES ('never')"))
            };
            SqlMigrator migrator = new SqlMigrator(factory, dialect, migrations, new SqlMigratorOptions { HistoryTableName = "mig_hist_fail" });
            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, dialect);

            MigrationException failure = Assert.Throws<MigrationException>(() => migrator.Migrate());
            Assert.Equal("20261005_0002_Broken", failure.MigrationId);
            Assert.NotNull(failure.InnerException);
            Assert.Equal(new[] { "20261005_0001_Ok" }, migrator.GetAppliedMigrations().Select(a => a.Id));
            Assert.Equal(new[] { "20261005_0002_Broken", "20261005_0003_Never" }, migrator.GetPendingMigrations().Select(m => m.Id));

            if (dialect.SupportsTransactionalDdl)
            {
                Assert.False(failure.MayBePartiallyApplied);
                Assert.False(reader.TableExists("mig_fail_partial"));
                Assert.Equal(0L, await CountAsync(factory, "mig_fail_items"));
            }
            else
            {
                Assert.True(failure.MayBePartiallyApplied);
                Assert.True(reader.TableExists("mig_fail_partial"));
            }

            MigrationException again = await Assert.ThrowsAsync<MigrationException>(() => migrator.MigrateAsync());
            Assert.Equal("20261005_0002_Broken", again.MigrationId);
            Assert.Single(await migrator.GetAppliedMigrationsAsync());
            Assert.Equal(0L, await CountAsync(factory, "mig_fail_items", "never"));

            Console.WriteLine("     Failure handled (transactional DDL: " + dialect.SupportsTransactionalDdl + "): " + failure.Message.Split('.')[0]);
        }

        /// <summary>
        /// Two migrators racing against the same database apply each migration exactly once.
        /// </summary>
        [Fact]
        public async Task Migrate_ConcurrentMigratorsApplyEachMigrationOnce()
        {
            await using IConnectionFactory factoryA = _Provider.CreateConnectionFactory();
            await using IConnectionFactory factoryB = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factoryA, "mig_hist_race", "mig_race_log");
            ISqlDialect dialect = _Provider.Dialect;
            List<Migration> migrations = new List<Migration>
            {
                new DelegateMigration("20261005_0001_CreateLog", "Create log", ctx =>
                {
                    ctx.EnsureSchema(typeof(MigRaceLog));
                    Thread.Sleep(150);
                })
            };
            for (int i = 2; i <= 5; i++)
            {
                string id = "20261005_000" + i.ToString(CultureInfo.InvariantCulture) + "_Log";
                migrations.Add(new DelegateMigration(id, "Log " + i.ToString(CultureInfo.InvariantCulture), ctx =>
                {
                    ctx.ExecuteSql("INSERT INTO " + dialect.QuoteIdentifier("mig_race_log") + " (" + dialect.QuoteIdentifier("migration_id") + ") VALUES (@p0)", id);
                    Thread.Sleep(50);
                }));
            }

            SqlMigratorOptions options = new SqlMigratorOptions { HistoryTableName = "mig_hist_race", LockPollIntervalMilliseconds = 50 };
            SqlMigrator first = new SqlMigrator(factoryA, dialect, migrations, options);
            SqlMigrator second = new SqlMigrator(factoryB, dialect, migrations, options);

            Task<MigrationRunResult> runA = Task.Run(() => first.MigrateAsync());
            Task<MigrationRunResult> runB = Task.Run(() => second.Migrate());
            MigrationRunResult[] results = await Task.WhenAll(runA, runB);

            List<string> applied = results.SelectMany(r => r.Applied).Select(a => a.Id).ToList();
            Assert.Equal(5, applied.Count);
            Assert.Equal(5, applied.Distinct(StringComparer.Ordinal).Count());
            Assert.Equal(5, first.GetAppliedMigrations().Count);
            Assert.Equal(4L, await CountAsync(factoryA, "mig_race_log"));
            Assert.Equal(4L, ToInt64(await ScalarAsync(factoryA, "SELECT COUNT(DISTINCT " + Q("migration_id") + ") FROM " + Q("mig_race_log"))));

            Console.WriteLine("     Migrator A applied " + results[0].Applied.Count + ", migrator B applied " + results[1].Applied.Count);
        }

        /// <summary>
        /// Script generation renders pending migrations (with inlined, escaped parameters, history inserts and
        /// transactions where supported) and the schema diff, without executing anything.
        /// </summary>
        [Fact]
        public async Task GenerateScript_RendersWithoutExecuting()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_hist_script", "mig_script_items");
            ISqlDialect dialect = _Provider.Dialect;
            SqlMigrator migrator = new SqlMigrator(factory, dialect, new SqlMigratorOptions { HistoryTableName = "mig_hist_script" })
                .AddMigration(new DelegateMigration("20261005_0001_CreateItems", "Create items", ctx => ctx.EnsureSchema(typeof(MigScriptItem))))
                .AddMigration(new DelegateMigration("20261005_0002_SeedItem", "Seed item", ctx =>
                {
                    Assert.True(ctx.IsScripting);
                    Assert.Null(ctx.Transaction);
                    ctx.ExecuteSql("INSERT INTO " + dialect.QuoteIdentifier("mig_script_items") + " (" + dialect.QuoteIdentifier("title") + ") VALUES (@p0)", "O'Brien");
                }));

            string script = migrator.GenerateScript();
            Assert.Contains("Migration 20261005_0001_CreateItems: Create items", script);
            Assert.Contains("CREATE TABLE", script);
            Assert.Contains("idx_mig_script_items_title", script);
            Assert.Contains("O''Brien", script);
            Assert.DoesNotContain("@p0", script);
            Assert.Contains("INSERT INTO " + dialect.QuoteIdentifier("mig_hist_script"), script);
            Assert.Contains(dialect.CurrentUtcTimestampSql, script);
            if (dialect.SupportsTransactionalDdl) Assert.Contains(dialect.ScriptBeginTransactionSql, script);
            if (dialect.ScriptBatchSeparator != null) Assert.Contains(Environment.NewLine + dialect.ScriptBatchSeparator + Environment.NewLine, script);
            Assert.Equal(script, await migrator.GenerateScriptAsync());

            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, dialect);
            Assert.False(reader.TableExists("mig_hist_script"));
            Assert.False(reader.TableExists("mig_script_items"));
            Assert.Equal(2, migrator.GetPendingMigrations().Count);

            string syncScript = await migrator.GenerateSyncScriptAsync(new[] { typeof(MigScriptItem) });
            Assert.Contains("Create table mig_script_items", syncScript);
            Assert.Contains("idx_mig_script_items_title", syncScript);
            Assert.False(reader.TableExists("mig_script_items"));

            Console.WriteLine("     Generated a " + script.Length + "-character migration script:");
            Console.WriteLine(script);
        }

        /// <summary>
        /// Applied migrations are reverted newest-first with Down; unregistered applied migrations block a rollback
        /// before any change.
        /// </summary>
        [Fact]
        public async Task RollbackTo_RevertsWithDown()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "mig_hist_down", "mig_down_items");
            ISqlDialect dialect = _Provider.Dialect;
            SqlMigratorOptions options = new SqlMigratorOptions { HistoryTableName = "mig_hist_down" };
            Migration create = new DelegateMigration(
                "20261005_0001_CreateDown", "Create",
                ctx => ctx.EnsureSchema(typeof(MigDownItem)),
                ctx => ctx.ExecuteSql(RelTestHelpers.DropTableSql(dialect, "mig_down_items")));
            Migration seed = new DelegateMigration(
                "20261005_0002_SeedDown", "Seed",
                ctx => ctx.ExecuteSql("INSERT INTO " + dialect.QuoteIdentifier("mig_down_items") + " (" + dialect.QuoteIdentifier("name") + ") VALUES (@p0)", "seeded"),
                ctx => ctx.ExecuteSql("DELETE FROM " + dialect.QuoteIdentifier("mig_down_items")));
            SqlMigrator migrator = new SqlMigrator(factory, dialect, new[] { create, seed }, options);
            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, dialect);
            Assert.True(create.SupportsDown);

            Assert.Equal(2, migrator.Migrate().Applied.Count);
            Assert.Equal(1L, await CountAsync(factory, "mig_down_items"));

            SqlMigrator partial = new SqlMigrator(factory, dialect, new[] { create }, options);
            Assert.Throws<InvalidOperationException>(() => partial.RollbackTo(null));
            Assert.Equal(2, migrator.GetAppliedMigrations().Count);

            MigrationRunResult toFirst = migrator.RollbackTo("20261005_0001_CreateDown");
            Assert.Equal(new[] { "20261005_0002_SeedDown" }, toFirst.Reverted);
            Assert.Equal(0L, await CountAsync(factory, "mig_down_items"));
            Assert.Equal(new[] { "20261005_0001_CreateDown" }, migrator.GetAppliedMigrations().Select(a => a.Id));

            MigrationRunResult all = await migrator.RollbackToAsync(null);
            Assert.Equal(new[] { "20261005_0001_CreateDown" }, all.Reverted);
            Assert.False(reader.TableExists("mig_down_items"));
            Assert.Empty(migrator.GetAppliedMigrations());

            Assert.Equal(2, (await migrator.MigrateAsync()).Applied.Count);
            Assert.Equal(1L, await CountAsync(factory, "mig_down_items"));

            Console.WriteLine("     Reverted and re-applied 2 migrations");
        }

        /// <summary>
        /// The pure diff and dialect helpers: create-table plans for missing tables, the NOT NULL default rule, literal
        /// escaping, and type normalization.
        /// </summary>
        [Fact]
        public void SchemaDiffer_PlansAndLiteralsWithoutDatabase()
        {
            ISqlDialect dialect = _Provider.Dialect;
            SchemaDiff plan = SchemaDiffer.Compare(dialect, new[] { typeof(MigAllTypes) }, new Dictionary<string, TableSchema>());
            Assert.Equal(MigrationOperationKind.CreateTable, plan.Operations[0].Kind);
            Assert.Equal(2, plan.Operations.Count(o => o.Kind == MigrationOperationKind.CreateIndex));
            Assert.Empty(plan.Differences);
            Assert.Throws<ArgumentException>(() => SchemaDiffer.Compare(dialect, new[] { typeof(MigWidgetV1), typeof(MigWidgetV2) }, new Dictionary<string, TableSchema>()));

            EntityMetadata widget = EntityMetadata.For<MigWidgetV2>();
            Assert.Equal("0", SchemaDiffer.GetDefaultLiteral(dialect, widget.FindColumnByName("quantity")!));
            Assert.Equal(dialect.FormatLiteral(dialect.Converter.ConvertToDatabase(true, widget.FindColumnByName("is_active"))), SchemaDiffer.GetDefaultLiteral(dialect, widget.FindColumnByName("is_active")!));
            Assert.Null(SchemaDiffer.GetDefaultLiteral(dialect, widget.FindColumnByName("label")!));
            Assert.Null(SchemaDiffer.GetDefaultLiteral(dialect, widget.FindColumnByName("quantity")!, new SchemaSyncOptions { UseClrDefaultsForNotNullColumns = false }));

            Assert.Equal("NULL", dialect.FormatLiteral(null));
            Assert.Equal("42", dialect.FormatLiteral(42));
            Assert.Contains("O''Brien", dialect.FormatLiteral("O'Brien"));

            ColumnMetadata name = EntityMetadata.For<MigAllTypes>().FindColumnByName("name")!;
            string declared = dialect.GetColumnType(name);
            Assert.Equal(dialect.NormalizeColumnType(declared), dialect.NormalizeColumnType("  " + declared.ToLowerInvariant().Replace("(", " ( ") + " "));

            Console.WriteLine("     Planned " + plan.Operations.Count + " operations for a missing table");
        }

        #endregion

        #region Private-Methods

        private string Q(string identifier)
        {
            return _Provider.Dialect.QuoteIdentifier(identifier);
        }

        private async Task DropTablesAsync(IConnectionFactory factory, params string[] tables)
        {
            foreach (string table in tables) await ExecuteAsync(factory, RelTestHelpers.DropTableSql(_Provider.Dialect, table));
        }

        private static async Task ExecuteAsync(IConnectionFactory factory, string sql)
        {
            await using DbConnection connection = await factory.OpenConnectionAsync();
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        private static async Task<object?> ScalarAsync(IConnectionFactory factory, string sql)
        {
            await using DbConnection connection = await factory.OpenConnectionAsync();
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            object? value = await command.ExecuteScalarAsync();
            return value == DBNull.Value ? null : value;
        }

        private async Task<long> CountAsync(IConnectionFactory factory, string table, string? name = null)
        {
            string sql = "SELECT COUNT(*) FROM " + Q(table);
            if (name != null) sql += " WHERE " + Q("name") + " = '" + name + "'";
            return ToInt64(await ScalarAsync(factory, sql));
        }

        private static long ToInt64(object? value)
        {
            if (value == null) return -1;
            if (value is bool flag) return flag ? 1 : 0;
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        #endregion
    }
}
