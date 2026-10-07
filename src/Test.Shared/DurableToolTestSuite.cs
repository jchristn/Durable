namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Durable.Tool;
    using Test.Shared.CliFixtures.Entities;
    using Test.Shared.CliFixtures.Generated;
    using Test.Shared.CliFixtures.Mapped;
    using Test.Shared.CliFixtures.Scaffold;
    using Xunit;

    /// <summary>
    /// Drives the durable command-line tool in-process (<see cref="DurableCli.RunAsync(string[], DurableCliContext, CancellationToken)"/>)
    /// against the configured provider: help and argument errors, migrate/status/rollback, scripts, schema diff/sync,
    /// migrations add (template and generated from a diff) and scaffold. Generated C# is compiled with Roslyn and executed.
    /// The user assembly is Test.Shared itself, with discovery scoped to the Test.Shared.CliFixtures namespaces.
    /// </summary>
    public class DurableToolTestSuite
    {
        #region Private-Members

        private const string _MigrationsNamespace = "Test.Shared.CliFixtures.Migrations";
        private const string _EntitiesNamespace = "Test.Shared.CliFixtures.Entities";
        private const string _GeneratedNamespace = "Test.Shared.CliFixtures.Generated";
        private const string _MappedNamespace = "Test.Shared.CliFixtures.Mapped";
        private const string _HistoryTable = "__durable_cli_history";
        private const string _First = "20260101000001_CreateItems";
        private const string _Second = "20260101000002_AddNote";
        private const string _Third = "20260101000003_IndexName";

        private readonly IRepositoryProvider _Provider;
        private readonly string _AssemblyPath = typeof(CliCustomer).Assembly.Location;
        private static int _CompileCounter = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="DurableToolTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public DurableToolTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// The overview, group and command help pages are complete and exit 0; no arguments prints help and exits 1.
        /// </summary>
        [Fact]
        public async Task Help_ShowsOverviewGroupsAndCommands()
        {
            CliRunResult overview = await RunAsync(null, "--help");
            Expect(overview, ExitCodes.Success);
            foreach (string command in new[] { "migrate", "rollback", "status", "script", "migrations add <Name>", "migrations list", "schema diff", "schema sync", "scaffold" })
                Assert.Contains(command, overview.Output);
            Assert.Contains("DURABLE_PROVIDER", overview.Output);

            CliRunResult empty = await RunAsync(null);
            Expect(empty, ExitCodes.CommandError);
            Assert.Contains("Usage: durable <command>", empty.Error);

            CliRunResult version = await RunAsync(null, "--version");
            Expect(version, ExitCodes.Success);
            Assert.Equal(DurableCli.Version, version.Output.Trim());
            Assert.StartsWith("0.7.0", DurableCli.Version);

            CliRunResult migrate = await RunAsync(null, "migrate", "--help");
            Expect(migrate, ExitCodes.Success);
            Assert.Contains("--target <id>", migrate.Output);
            Assert.Contains("--provider <name>", migrate.Output);
            Assert.Contains("--migrations-namespace <ns>", migrate.Output);
            Assert.Contains("Examples:", migrate.Output);

            CliRunResult add = await RunAsync(null, "help", "migrations", "add");
            Expect(add, ExitCodes.Success);
            Assert.Contains("Usage: durable migrations add <Name> [options]", add.Output);
            Assert.Contains("--empty", add.Output);

            CliRunResult sync = await RunAsync(null, "schema", "sync", "-h");
            Expect(sync, ExitCodes.Success);
            Assert.Contains("--dry-run", sync.Output);
            Assert.Contains("--mapping-source <type>", sync.Output);
            Assert.Contains("--allow-destructive", sync.Output);

            CliRunResult group = await RunAsync(null, "migrations", "--help");
            Expect(group, ExitCodes.Success);
            Assert.Contains("migrations add <Name>", group.Output);
            Assert.Contains("migrations list", group.Output);

            CliRunResult scaffold = await RunAsync(null, "scaffold", "--help");
            Expect(scaffold, ExitCodes.Success);
            Assert.Contains("--tables <names>", scaffold.Output);

            CliRunResult list = await RunAsync(null, "migrations", "list", "--help");
            Expect(list, ExitCodes.Success);
            Assert.Contains("alias of 'durable status'", list.Output);
        }

        /// <summary>
        /// The wire-compatible databases are providers of their own (MariaDB, CockroachDB, YugabyteDB, with aliases):
        /// help lists them and each name is accepted (an invalid connection string fails in the driver, not as an unknown provider).
        /// </summary>
        [Fact]
        public async Task WireCompatibleProviders_AreListedAndAccepted()
        {
            CliRunResult help = await RunAsync(null, "status", "--help");
            Expect(help, ExitCodes.Success);
            foreach (string name in new[] { "mariadb", "cockroachdb", "yugabytedb" })
                Assert.True(help.Output.Contains(name, StringComparison.Ordinal), "Expected '" + name + "' in: " + help.Output);

            foreach (string alias in new[] { "mariadb", "cockroachdb", "cockroach", "crdb", "yugabytedb", "yugabyte", "ysql" })
            {
                CliRunResult result = await RunAsync(null, "status", "--provider", alias, "--connection", "not a connection string", "--assembly", _AssemblyPath);
                Assert.True(result.ExitCode != ExitCodes.Success, "Expected a failure for provider '" + alias + "' but got " + result);
                Assert.True(result.Error.StartsWith("error: ", StringComparison.Ordinal), "Errors start with 'error: ': " + result);
                Assert.False(result.Error.Contains("Unknown provider", StringComparison.Ordinal), "Provider '" + alias + "' was not recognized: " + result);
            }
        }

        /// <summary>
        /// Invalid arguments and missing settings exit 1 with a clear message on the error stream and nothing on the output.
        /// </summary>
        [Fact]
        public async Task BadArguments_ExitWithCommandErrorAndMessage()
        {
            await AssertErrorAsync(new[] { "migrat" }, "Unknown command 'migrat'. Did you mean 'migrate'?");
            await AssertErrorAsync(new[] { "migrations", "remove" }, "Unknown command 'migrations remove'");
            await AssertErrorAsync(new[] { "schema" }, "'durable schema' needs a subcommand");
            await AssertErrorAsync(new[] { "status", "--provdier", "sqlite" }, "Unknown option '--provdier' for 'durable status'. Did you mean --provider?");
            await AssertErrorAsync(new[] { "status", "--provider" }, "Option --provider requires a value");
            await AssertErrorAsync(new[] { "status", "--provider", "sqlite", "--provider", "mysql" }, "given more than once");
            await AssertErrorAsync(new[] { "status", "--dry-run" }, "Unknown option '--dry-run'");
            await AssertErrorAsync(new[] { "schema", "sync", "--dry-run=yes" }, "does not take a value");
            await AssertErrorAsync(new[] { "status", "extra" }, "Unexpected argument 'extra'");
            await AssertErrorAsync(new[] { "migrations", "add" }, "requires <Name>");
            await AssertErrorAsync(new[] { "migrations", "add", "Not-Valid" }, "not a valid migration name");
            await AssertErrorAsync(new[] { "status", "--assembly", _AssemblyPath }, "No database provider");
            await AssertErrorAsync(new[] { "status", "--provider", "sqlite", "--assembly", _AssemblyPath }, "No connection string");
            await AssertErrorAsync(new[] { "status", "--provider", "db2", "--connection", "x", "--assembly", _AssemblyPath }, "Unknown provider 'db2'");
            await AssertErrorAsync(Migrations("rollback"), "rollback requires --target");
            await AssertErrorAsync(Database("status", "--assembly", _AssemblyPath, "--migrations-namespace", _MigrationsNamespace, "--history-table", "bad name"), "Invalid --history-table");
            await AssertErrorAsync(Database("status", "--assembly", Path.Combine(Path.GetTempPath(), "no-such-assembly-" + Guid.NewGuid().ToString("N") + ".dll")), "was not found");
            await AssertErrorAsync(new[] { "status", "--assembly", "a.dll", "--project", "b.csproj" }, "either --assembly or --project");
            await AssertErrorAsync(new[] { "status", "--config", "missing-durable.json" }, "was not found");
            await AssertErrorAsync(Database("status"), "No project file (.csproj) found");
            await AssertErrorAsync(Database("schema", "diff", "--assembly", _AssemblyPath, "--entities-namespace", "No.Such.Namespace"), "No entity types found");
            await AssertErrorAsync(Database("schema", "diff", "--assembly", _AssemblyPath, "--entities", "NoSuchEntity"), "Entity type 'NoSuchEntity' was not found");
            await AssertErrorAsync(Migrations("migrate", "--target", "nope"), "Unknown migration 'nope'");
            await AssertErrorAsync(Migrations("rollback", "--target", "nope"), "Unknown migration 'nope'");
        }

        /// <summary>
        /// migrate (with and without --target), status and rollback round-trip the fixture migrations, and the schema
        /// follows each step.
        /// </summary>
        [Fact]
        public async Task MigrateStatusRollback_RoundTrip()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await ResetMigrationsAsync(factory);
            try
            {
                CliRunResult status = await RunAsync(null, Migrations("status"));
                Expect(status, ExitCodes.Success);
                Assert.Contains("history table " + _HistoryTable, status.Output);
                Assert.Matches(new Regex("Pending\\s+" + _First), status.Output);
                Assert.Contains("0 applied, 3 pending.", status.Output);

                CliRunResult partial = await RunAsync(null, Migrations("migrate", "--target", _Second));
                Expect(partial, ExitCodes.Success);
                Assert.Contains("Applied  " + _First, partial.Output);
                Assert.Contains("Applied  " + _Second, partial.Output);
                Assert.DoesNotContain(_Third, partial.Output);
                Assert.Contains("Applied 2 migration(s)", partial.Output);

                CliRunResult afterPartial = await RunAsync(null, Migrations("migrations", "list"));
                Expect(afterPartial, ExitCodes.Success);
                Assert.Matches(new Regex("Applied\\s+" + _Second), afterPartial.Output);
                Assert.Matches(new Regex("Pending\\s+" + _Third), afterPartial.Output);
                Assert.Contains("2 applied, 1 pending.", afterPartial.Output);

                CliRunResult rest = await RunAsync(null, Migrations("migrate"));
                Expect(rest, ExitCodes.Success);
                Assert.Contains("Applied  " + _Third, rest.Output);
                Assert.Contains("Applied 1 migration(s)", rest.Output);

                CliRunResult again = await RunAsync(null, Migrations("migrate"));
                Expect(again, ExitCodes.Success);
                Assert.Contains("No pending migrations", again.Output);

                DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
                TableSchema? table = await reader.ReadTableAsync("cli_items");
                Assert.NotNull(table);
                Assert.NotNull(table!.FindColumn("note"));
                Assert.NotNull(table.FindIndex("idx_cli_items_name"));

                CliRunResult earlierTarget = await RunAsync(null, Migrations("migrate", "--target", _First));
                Expect(earlierTarget, ExitCodes.Success);
                Assert.Contains("already applied", earlierTarget.Error);

                CliRunResult rollback = await RunAsync(null, Migrations("rollback", "--target", _First));
                Expect(rollback, ExitCodes.Success);
                Assert.Contains("Reverted " + _Third, rollback.Output);
                Assert.Contains("Reverted " + _Second, rollback.Output);
                Assert.True(rollback.Output.IndexOf(_Third, StringComparison.Ordinal) < rollback.Output.IndexOf(_Second, StringComparison.Ordinal), "Newest migrations revert first.");
                table = await reader.ReadTableAsync("cli_items");
                Assert.NotNull(table);
                Assert.Null(table!.FindColumn("note"));
                Assert.Null(table.FindIndex("idx_cli_items_name"));

                CliRunResult nothing = await RunAsync(null, Migrations("rollback", "--target", _First));
                Expect(nothing, ExitCodes.Success);
                Assert.Contains("Nothing to revert", nothing.Output);

                CliRunResult all = await RunAsync(null, Migrations("rollback", "--target", "0"));
                Expect(all, ExitCodes.Success);
                Assert.Contains("Reverted " + _First, all.Output);
                Assert.False(await reader.TableExistsAsync("cli_items"));

                CliRunResult final = await RunAsync(null, Migrations("status"));
                Expect(final, ExitCodes.Success);
                Assert.Contains("0 applied, 3 pending.", final.Output);
            }
            finally
            {
                await ResetMigrationsAsync(factory);
            }
        }

        /// <summary>
        /// script writes pending migrations without executing them, and --from/--to script a range regardless of history,
        /// to standard output or a file.
        /// </summary>
        [Fact]
        public async Task Script_PendingAndRange()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await ResetMigrationsAsync(factory);
            string directory = CreateTempDirectory();
            try
            {
                CliRunResult pending = await RunAsync(directory, Migrations("script"));
                Expect(pending, ExitCodes.Success);
                Assert.Contains("Migration " + _First, pending.Output);
                Assert.Contains("Migration " + _Third, pending.Output);
                Assert.Contains(_Provider.Dialect.QuoteIdentifier("cli_items"), pending.Output);
                Assert.Contains(_Provider.Dialect.QuoteIdentifier(_HistoryTable), pending.Output);
                DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
                Assert.False(await reader.TableExistsAsync("cli_items"), "script must not execute migrations");

                Expect(await RunAsync(null, Migrations("migrate", "--target", _First)), ExitCodes.Success);
                CliRunResult remaining = await RunAsync(directory, Migrations("script"));
                Expect(remaining, ExitCodes.Success);
                Assert.DoesNotContain("Migration " + _First, remaining.Output);
                Assert.Contains("Migration " + _Second, remaining.Output);
                Assert.Contains("Migration " + _Third, remaining.Output);

                CliRunResult range = await RunAsync(directory, Migrations("script", "--from", _First, "--to", _Second, "--output", "out/range.sql"));
                Expect(range, ExitCodes.Success);
                string path = Path.Combine(directory, "out", "range.sql");
                Assert.Contains("Wrote", range.Output);
                Assert.True(File.Exists(path));
                string script = await File.ReadAllTextAsync(path);
                Assert.Contains("Migration " + _Second, script);
                Assert.DoesNotContain("Migration " + _First, script);
                Assert.DoesNotContain("Migration " + _Third, script);

                CliRunResult fromStart = await RunAsync(directory, Migrations("script", "--from", "0", "--to", _First));
                Expect(fromStart, ExitCodes.Success);
                Assert.Contains("Migration " + _First, fromStart.Output);
                Assert.DoesNotContain("Migration " + _Second, fromStart.Output);

                await AssertErrorAsync(Migrations("script", "--from", "nope"), "Unknown migration 'nope' for --from");
                await AssertErrorAsync(Migrations("script", "--from", _Second, "--to", _First), "must sort before");
            }
            finally
            {
                await ResetMigrationsAsync(factory);
                DeleteDirectory(directory);
            }
        }

        /// <summary>
        /// schema diff reports additive and destructive operations, --sql and sync --dry-run print SQL without changing the
        /// database, sync applies additive changes, and --allow-destructive drops unmapped columns.
        /// </summary>
        [Fact]
        public async Task SchemaDiffAndSync_DryRunThenApply()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "cli_customers", "cli_orders");
            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
            try
            {
                CliRunResult diff = await RunAsync(null, Entities("schema", "diff"));
                Expect(diff, ExitCodes.Success);
                Assert.Contains("+ Create table cli_customers", diff.Output);
                Assert.Contains("+ Create table cli_orders", diff.Output);
                Assert.Contains("idx_cli_customers_name", diff.Output);
                Assert.Contains("2 entity type(s)", diff.Output);

                CliRunResult sql = await RunAsync(null, Entities("schema", "diff", "--sql"));
                Expect(sql, ExitCodes.Success);
                Assert.Contains("CREATE TABLE", sql.Output);

                CliRunResult dryRun = await RunAsync(null, Entities("schema", "sync", "--dry-run"));
                Expect(dryRun, ExitCodes.Success);
                Assert.Contains("CREATE TABLE", dryRun.Output);
                Assert.False(await reader.TableExistsAsync("cli_customers"), "--dry-run must not change the database");

                CliRunResult sync = await RunAsync(null, Entities("schema", "sync"));
                Expect(sync, ExitCodes.Success);
                Assert.Contains("Applied  Create table cli_customers", sync.Output);
                Assert.True(await reader.TableExistsAsync("cli_customers"));
                Assert.True(await reader.TableExistsAsync("cli_orders"));

                CliRunResult clean = await RunAsync(null, Entities("schema", "diff"));
                Expect(clean, ExitCodes.Success);
                Assert.Contains("matches", clean.Output);

                await ExecuteAsync(factory, "ALTER TABLE " + Q("cli_customers") + " ADD " + Q("legacy") + " INT NULL");
                CliRunResult destructive = await RunAsync(null, Entities("schema", "diff"));
                Expect(destructive, ExitCodes.Success);
                Assert.Contains("- Drop unmapped column cli_customers.legacy", destructive.Output);

                CliRunResult skipped = await RunAsync(null, Entities("schema", "sync"));
                Expect(skipped, ExitCodes.Success);
                Assert.Contains("Skipped  Drop unmapped column cli_customers.legacy", skipped.Output);
                Assert.NotNull((await reader.ReadTableAsync("cli_customers"))!.FindColumn("legacy"));

                CliRunResult dropped = await RunAsync(null, Entities("schema", "sync", "--allow-destructive"));
                Expect(dropped, ExitCodes.Success);
                Assert.Contains("Applied  Drop unmapped column cli_customers.legacy", dropped.Output);
                Assert.Null((await reader.ReadTableAsync("cli_customers"))!.FindColumn("legacy"));

                CliRunResult single = await RunAsync(null, Database("schema", "diff", "--assembly", _AssemblyPath, "--entities", "CliCustomer"));
                Expect(single, ExitCodes.Success);
                Assert.Contains("1 entity type(s)", single.Output);
            }
            finally
            {
                await DropTablesAsync(factory, "cli_customers", "cli_orders");
            }
        }

        /// <summary>
        /// --mapping-source registers an IEntityMappingSource from the user's assembly: the classes it describes are discovered
        /// as entities without [Entity], schema diff shows the translated table, migrations add generates a migration that
        /// compiles and applies exactly that schema, and a bad source type is reported.
        /// </summary>
        [Fact]
        public async Task MappingSource_DiscoversAndMigratesSourceMappedTypes()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            const string history = "__durable_cli_mapped_history";
            await DropTablesAsync(factory, "cli_mapped_gadgets", history);
            string directory = CreateTempDirectory();
            try
            {
                await AssertErrorAsync(Database("schema", "diff", "--assembly", _AssemblyPath, "--entities-namespace", _MappedNamespace), "No entity types found");
                await AssertErrorAsync(Database("schema", "diff", "--assembly", _AssemblyPath, "--mapping-source", "NoSuchSource"), "Mapping source type 'NoSuchSource' was not found");
                await AssertErrorAsync(Database("schema", "diff", "--assembly", _AssemblyPath, "--mapping-source", "CliMappedGadget"), "must be a concrete IEntityMappingSource class");

                CliRunResult diff = await RunAsync(null, Database("schema", "diff", "--assembly", _AssemblyPath, "--entities-namespace", _MappedNamespace, "--mapping-source", nameof(MapAttributeMappingSource)));
                Expect(diff, ExitCodes.Success);
                Assert.Contains("+ Create table cli_mapped_gadgets", diff.Output);
                Assert.Contains("idx_cli_mapped_gadgets_kind_name", diff.Output);
                Assert.Contains("1 entity type(s)", diff.Output);

                CliRunResult added = await RunAsync(directory, Database(
                    "migrations", "add", "CreateGadgets", "--assembly", _AssemblyPath, "--entities-namespace", _MappedNamespace,
                    "--mapping-source", typeof(MapAttributeMappingSource).FullName!, "--migrations-namespace", _MigrationsNamespace,
                    "--output-dir", "Gen", "--namespace", "Cli.Generated.Mapped"));
                Expect(added, ExitCodes.Success);
                string file = Assert.Single(Directory.GetFiles(Path.Combine(directory, "Gen"), "*_CreateGadgets.cs"));
                string code = await File.ReadAllTextAsync(file);
                Assert.Contains("// Create table cli_mapped_gadgets", code);
                Assert.Contains("gadget_name", code);

                Assembly compiled = GeneratedCodeCompiler.CompileAndLoad(NextAssemblyName(), new[] { code });
                Migration migration = (Migration)Activator.CreateInstance(compiled.GetType("Cli.Generated.Mapped.CreateGadgets", true)!)!;
                SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect, new[] { migration }, new SqlMigratorOptions { HistoryTableName = history });
                Assert.Single((await migrator.MigrateAsync()).Applied);

                DurableMapping.Register<CliMappedGadget>(MsMappingRegistration.Source);
                SchemaDiff after = await migrator.DiffSchemaAsync(new[] { EntityMetadata.For<CliMappedGadget>() });
                Assert.True(after.IsEmpty, "Generated migration should produce the mapped schema: " + Describe(after));

                CliRunResult clean = await RunAsync(null, Database("schema", "diff", "--assembly", _AssemblyPath, "--entities-namespace", _MappedNamespace, "--mapping-source", nameof(MapAttributeMappingSource)));
                Expect(clean, ExitCodes.Success);
                Assert.Contains("matches", clean.Output);
            }
            finally
            {
                await DropTablesAsync(factory, "cli_mapped_gadgets", history);
                DeleteDirectory(directory);
            }
        }

        /// <summary>
        /// migrations add without a database writes an empty, compilable template with a chronological Id, in the default
        /// or given namespace, and refuses duplicate names.
        /// </summary>
        [Fact]
        public async Task MigrationsAdd_EmptyTemplateCompiles()
        {
            string directory = CreateTempDirectory();
            try
            {
                DateTime before = DateTime.UtcNow.AddSeconds(-1);
                CliRunResult added = await RunAsync(directory, "migrations", "add", "AddThings", "--output-dir", "Data/Migrations", "--namespace", "Cli.Generated.Empty");
                Expect(added, ExitCodes.Success);
                Assert.Contains("empty template", added.Output);
                string file = Assert.Single(Directory.GetFiles(Path.Combine(directory, "Data", "Migrations"), "*.cs"));
                string id = Path.GetFileNameWithoutExtension(file);
                Assert.Matches(new Regex("^\\d{14}_AddThings$"), id);
                DateTime stamp = DateTime.ParseExact(id.Substring(0, 14), "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture);
                Assert.True(stamp >= before.AddSeconds(-1) && stamp <= DateTime.UtcNow.AddSeconds(1), "The Id uses the current UTC time.");

                string code = await File.ReadAllTextAsync(file);
                Assert.StartsWith("namespace Cli.Generated.Empty", code);
                Assert.Contains("public class AddThings : Migration", code);
                Assembly compiled = GeneratedCodeCompiler.CompileAndLoad(NextAssemblyName(), new[] { code });
                Migration migration = (Migration)Activator.CreateInstance(compiled.GetType("Cli.Generated.Empty.AddThings", true)!)!;
                Assert.Equal(id, migration.Id);
                Assert.False(migration.SupportsDown);

                CliRunResult second = await RunAsync(directory, "migrations", "add", "SecondThing");
                Expect(second, ExitCodes.Success);
                string secondFile = Assert.Single(Directory.GetFiles(Path.Combine(directory, "Migrations"), "*_SecondThing.cs"));
                Assert.StartsWith("namespace Migrations", await File.ReadAllTextAsync(secondFile));

                CliRunResult duplicate = await RunAsync(directory, "migrations", "add", "AddThings", "--output-dir", "Data/Migrations");
                Expect(duplicate, ExitCodes.CommandError);
                Assert.Contains("already exists", duplicate.Error);
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        /// <summary>
        /// migrations add with a database generates Up from the schema diff and a reversing Down; the generated class
        /// compiles, applies the entity's schema exactly, and rolls it back.
        /// </summary>
        [Fact]
        public async Task MigrationsAdd_FromDiffCompilesAndRoundTrips()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            const string history = "__durable_cli_gen_history";
            await DropTablesAsync(factory, "cli_gen_widgets", history);
            string directory = CreateTempDirectory();
            try
            {
                CliRunResult added = await RunAsync(directory, Database(
                    "migrations", "add", "CreateWidgets", "--assembly", _AssemblyPath, "--entities-namespace", _GeneratedNamespace,
                    "--migrations-namespace", _MigrationsNamespace, "--output-dir", "Gen", "--namespace", "Cli.Generated.Diff"));
                Expect(added, ExitCodes.Success);
                Assert.Contains("operation(s) from the differences between 1 entity type(s)", added.Output);
                string file = Assert.Single(Directory.GetFiles(Path.Combine(directory, "Gen"), "*_CreateWidgets.cs"));
                string code = await File.ReadAllTextAsync(file);
                Assert.Contains("// Create table cli_gen_widgets", code);
                Assert.Contains("context.ExecuteSqlRaw(", code);
                Assert.Contains("public override void Down(MigrationContext context)", code);
                Assert.Contains("RepositoryType." + RepositoryTypeField(), code);

                Assembly compiled = GeneratedCodeCompiler.CompileAndLoad(NextAssemblyName(), new[] { code });
                Migration migration = (Migration)Activator.CreateInstance(compiled.GetType("Cli.Generated.Diff.CreateWidgets", true)!)!;
                Assert.Equal(Path.GetFileNameWithoutExtension(file), migration.Id);
                Assert.True(migration.SupportsDown);

                SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect, new[] { migration }, new SqlMigratorOptions { HistoryTableName = history });
                MigrationRunResult applied = await migrator.MigrateAsync();
                Assert.Single(applied.Applied);
                SchemaDiff diff = await migrator.DiffSchemaAsync(new[] { typeof(CliGeneratedWidget) });
                Assert.True(diff.IsEmpty, "Generated migration should produce the mapped schema: " + Describe(diff));

                CliRunResult noChanges = await RunAsync(directory, Database(
                    "migrations", "add", "NothingToDo", "--assembly", _AssemblyPath, "--entities-namespace", _GeneratedNamespace, "--output-dir", "Gen", "--namespace", "Cli.Generated.Diff"));
                Expect(noChanges, ExitCodes.Success);
                Assert.Contains("0 operation(s)", noChanges.Output);
                string emptyCode = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(Path.Combine(directory, "Gen"), "*_NothingToDo.cs")));
                Assert.Contains("matched the database", emptyCode);
                GeneratedCodeCompiler.CompileAndLoad(NextAssemblyName(), new[] { emptyCode });

                MigrationRunResult reverted = await migrator.RollbackToAsync(null);
                Assert.Single(reverted.Reverted);
                DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
                Assert.False(await reader.TableExistsAsync("cli_gen_widgets"));
            }
            finally
            {
                await DropTablesAsync(factory, "cli_gen_widgets", history);
                DeleteDirectory(directory);
            }
        }

        /// <summary>
        /// scaffold reverse-engineers a table into an entity class (keys, identity, nullability, lengths, indexes) that
        /// compiles and maps back to exactly the same schema; scaffolding every table also compiles, and existing files
        /// are protected unless --force is given.
        /// </summary>
        [Fact]
        public async Task Scaffold_GeneratesEntitiesThatRoundTrip()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await DropTablesAsync(factory, "cli_scaffold_items");
            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);
            await migrator.SyncSchemaAsync(new[] { typeof(CliScaffoldSource) });
            string directory = CreateTempDirectory();
            try
            {
                CliRunResult scaffolded = await RunAsync(directory, Database("scaffold", "--tables", "cli_scaffold_items", "--output-dir", "Ent", "--namespace", "Cli.Scaffolded"));
                Expect(scaffolded, ExitCodes.Success);
                string file = Path.Combine(directory, "Ent", "CliScaffoldItem.cs");
                Assert.True(File.Exists(file), scaffolded.ToString());
                string code = await File.ReadAllTextAsync(file);
                Assert.Contains("[Entity(\"cli_scaffold_items\")]", code);
                Assert.Contains("Flags.PrimaryKey | Flags.AutoIncrement", code);
                Assert.Contains("[Index(\"idx_cli_scaffold_name\")]", code);
                Assert.Contains("[CompositeIndex(\"idx_cli_scaffold_code_count\", \"code\", \"item_count\", IsUnique = true)]", code);
                Assert.Contains("? Notes { get; set; }", code);
                Assert.Contains("? Maybe { get; set; }", code);
                Assert.Contains("byte[]? Data { get; set; }", code);
                // A dialect that declares string columns nullable (Oracle stores an empty string as NULL) scaffolds string?.
                bool nameNullable = _Provider.Dialect.ColumnAllowsNull(EntityMetadata.For(typeof(CliScaffoldSource)).FindColumnByName("name")!);
                Assert.Contains(nameNullable ? "public string? Name { get; set; }" : "public string Name { get; set; } = string.Empty;", code);
                if (_Provider.Dialect.SupportsStringMaxLength) Assert.Contains("[Property(\"name\", Flags.String, 80)]", code);
                if (_Provider.DatabaseType != TestDatabaseType.Sqlite)
                {
                    Assert.Contains("public Guid Uid { get; set; }", code);
                    Assert.Contains("public bool Flag { get; set; }", code);
                }

                Assembly compiled = GeneratedCodeCompiler.CompileAndLoad(NextAssemblyName(), new[] { code });
                Type entity = compiled.GetType("Cli.Scaffolded.CliScaffoldItem", true)!;
                SchemaDiff diff = await migrator.DiffSchemaAsync(new[] { entity });
                Assert.True(diff.IsEmpty, "Scaffolded entity should map back to the same schema: " + Describe(diff));

                CliRunResult exists = await RunAsync(directory, Database("scaffold", "--tables", "cli_scaffold_items", "--output-dir", "Ent", "--namespace", "Cli.Scaffolded"));
                Expect(exists, ExitCodes.CommandError);
                Assert.Contains("already exist", exists.Error);
                Expect(await RunAsync(directory, Database("scaffold", "--tables", "cli_scaffold_items", "--output-dir", "Ent", "--namespace", "Cli.Scaffolded", "--force")), ExitCodes.Success);

                CliRunResult missing = await RunAsync(directory, Database("scaffold", "--tables", "cli_no_such_table"));
                Expect(missing, ExitCodes.CommandError);
                Assert.Contains("Table 'cli_no_such_table' was not found", missing.Error);

                CliRunResult everything = await RunAsync(directory, Database("scaffold", "--output-dir", "All", "--namespace", "Cli.ScaffoldedAll", "--history-table", _HistoryTable));
                Expect(everything, ExitCodes.Success);
                Assert.Contains("(table cli_scaffold_items)", everything.Output);
                List<string> sources = new List<string>();
                foreach (string path in Directory.GetFiles(Path.Combine(directory, "All"), "*.cs")) sources.Add(await File.ReadAllTextAsync(path));
                Assert.True(sources.Count >= 1);
                GeneratedCodeCompiler.CompileAndLoad(NextAssemblyName(), sources);
            }
            finally
            {
                await DropTablesAsync(factory, "cli_scaffold_items");
                DeleteDirectory(directory);
            }
        }

        /// <summary>
        /// Settings come from durable.json (relative paths resolved against the file), then DURABLE_PROVIDER /
        /// DURABLE_CONNECTION, with command-line options taking precedence; invalid settings files are reported.
        /// </summary>
        [Fact]
        public async Task Settings_FromDurableJsonEnvironmentAndCommandLine()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            await ResetMigrationsAsync(factory);
            string directory = CreateTempDirectory();
            try
            {
                Dictionary<string, object> settings = new Dictionary<string, object>
                {
                    ["provider"] = ProviderName(),
                    ["connection"] = _Provider.ConnectionString,
                    ["assembly"] = Path.GetRelativePath(directory, _AssemblyPath),
                    ["migrationsNamespace"] = _MigrationsNamespace,
                    ["historyTable"] = _HistoryTable
                };
                await File.WriteAllTextAsync(Path.Combine(directory, "durable.json"), JsonSerializer.Serialize(settings));

                CliRunResult fromFile = await RunAsync(directory, "status");
                Expect(fromFile, ExitCodes.Success);
                Assert.Contains("history table " + _HistoryTable, fromFile.Output);
                Assert.Contains("0 applied, 3 pending.", fromFile.Output);

                settings["provider"] = "bogus";
                await File.WriteAllTextAsync(Path.Combine(directory, "durable.json"), JsonSerializer.Serialize(settings));
                Expect(await RunAsync(directory, "status"), ExitCodes.CommandError);
                Expect(await RunAsync(directory, "status", "--provider", ProviderName()), ExitCodes.Success);
                Dictionary<string, string> environment = new Dictionary<string, string> { ["DURABLE_PROVIDER"] = ProviderName() };
                Expect(await RunAsync(directory, environment, "status"), ExitCodes.Success);

                string elsewhere = CreateTempDirectory();
                try
                {
                    Dictionary<string, string> full = new Dictionary<string, string>
                    {
                        ["DURABLE_PROVIDER"] = ProviderName(),
                        ["DURABLE_CONNECTION"] = _Provider.ConnectionString
                    };
                    CliRunResult fromEnvironment = await RunAsync(
                        elsewhere, full, "status", "--assembly", _AssemblyPath, "--migrations-namespace", _MigrationsNamespace, "--history-table", _HistoryTable);
                    Expect(fromEnvironment, ExitCodes.Success);
                    Assert.Contains("0 applied, 3 pending.", fromEnvironment.Output);

                    CliRunResult explicitConfig = await RunAsync(elsewhere, "status", "--config", Path.Combine(directory, "durable.json"), "--provider", ProviderName());
                    Expect(explicitConfig, ExitCodes.Success);

                    await File.WriteAllTextAsync(Path.Combine(elsewhere, "durable.json"), "{ \"provder\": \"sqlite\" }");
                    CliRunResult invalid = await RunAsync(elsewhere, "status");
                    Expect(invalid, ExitCodes.CommandError);
                    Assert.Contains("is invalid", invalid.Error);
                    Assert.Contains("Supported properties", invalid.Error);
                }
                finally
                {
                    DeleteDirectory(elsewhere);
                }

            }
            finally
            {
                await ResetMigrationsAsync(factory);
                DeleteDirectory(directory);
            }
        }

        #endregion

        #region Private-Methods

        private async Task<CliRunResult> RunAsync(string? workingDirectory, params string[] args)
        {
            return await RunAsync(workingDirectory, null, args);
        }

        private async Task<CliRunResult> RunAsync(string? workingDirectory, IReadOnlyDictionary<string, string>? environment, params string[] args)
        {
            string directory = workingDirectory ?? CreateTempDirectory();
            try
            {
                using StringWriter output = new StringWriter();
                using StringWriter error = new StringWriter();
                DurableCliContext context = new DurableCliContext(output, error, directory, environment ?? new Dictionary<string, string>());
                int exitCode = await DurableCli.RunAsync(args, context, CancellationToken.None);
                return new CliRunResult(exitCode, output.ToString(), error.ToString());
            }
            finally
            {
                if (workingDirectory == null) DeleteDirectory(directory);
            }
        }

        private async Task AssertErrorAsync(string[] args, string expectedMessage)
        {
            CliRunResult result = await RunAsync(null, args);
            Assert.True(result.ExitCode == ExitCodes.CommandError, "Expected exit 1 for 'durable " + string.Join(" ", args) + "' but got " + result);
            Assert.True(result.Error.Contains(expectedMessage, StringComparison.Ordinal), "Expected error containing '" + expectedMessage + "' but got " + result);
            Assert.True(result.Error.StartsWith("error: ", StringComparison.Ordinal), "Errors start with 'error: ': " + result);
        }

        private static void Expect(CliRunResult result, int exitCode)
        {
            Assert.True(result.ExitCode == exitCode, "Expected exit " + exitCode + " but got " + result);
        }

        private string[] Database(params string[] args)
        {
            return args.Concat(new[] { "--provider", ProviderName(), "--connection", _Provider.ConnectionString }).ToArray();
        }

        private string[] Migrations(params string[] args)
        {
            return Database(args).Concat(new[] { "--assembly", _AssemblyPath, "--migrations-namespace", _MigrationsNamespace, "--history-table", _HistoryTable }).ToArray();
        }

        private string[] Entities(params string[] args)
        {
            return Database(args).Concat(new[] { "--assembly", _AssemblyPath, "--entities-namespace", _EntitiesNamespace }).ToArray();
        }

        private string ProviderName()
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite: return "sqlite";
                case TestDatabaseType.DuckDb: return "duckdb";
                case TestDatabaseType.Postgres: return "postgres";
                case TestDatabaseType.CockroachDb: return "cockroachdb";
                case TestDatabaseType.YugabyteDb: return "yugabytedb";
                case TestDatabaseType.MySql: return "mysql";
                case TestDatabaseType.MariaDb: return "mariadb";
                case TestDatabaseType.SqlServer: return "sqlserver";
                case TestDatabaseType.Oracle: return "oracle";
                default: throw new InvalidOperationException("Unknown provider " + _Provider.DatabaseType);
            }
        }

        private string RepositoryTypeField()
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite: return "Sqlite";
                case TestDatabaseType.DuckDb: return "DuckDb";
                case TestDatabaseType.Postgres:
                case TestDatabaseType.CockroachDb:
                case TestDatabaseType.YugabyteDb: return "Postgres";
                case TestDatabaseType.MySql:
                case TestDatabaseType.MariaDb: return "MySql";
                case TestDatabaseType.Oracle: return "Oracle";
                default: return "SqlServer";
            }
        }

        private static string NextAssemblyName()
        {
            return "DurableToolGenerated" + Interlocked.Increment(ref _CompileCounter) + "_" + Guid.NewGuid().ToString("N");
        }

        private static string Describe(SchemaDiff diff)
        {
            return string.Join(" | ", diff.Operations.Select(o => o.ToString()).Concat(diff.Differences.Select(d => d.ToString())));
        }

        private async Task ResetMigrationsAsync(IConnectionFactory factory)
        {
            await DropTablesAsync(factory, "cli_items", _HistoryTable);
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

        private string Q(string identifier)
        {
            return _Provider.Dialect.QuoteIdentifier(identifier);
        }

        private static string CreateTempDirectory()
        {
            string path = Path.Combine(Path.GetTempPath(), "durable-tool-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);
            return path;
        }

        private static void DeleteDirectory(string path)
        {
            try
            {
                if (Directory.Exists(path)) Directory.Delete(path, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        #endregion
    }
}
