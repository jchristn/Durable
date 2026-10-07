namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Durable.SqlServer;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.Sqlite;
    using MySqlConnector;
    using Npgsql;
    using Xunit;

    /// <summary>
    /// Creates a brand-new database on the configured provider with CreateDatabaseIfNotExistsAsync (twice, to prove it
    /// is idempotent), then exercises InitializeTablesAsync, DatabaseSchemaReader.ReadTables[Async] and
    /// SqlMigrator.AddMigrations inside it, and finally drops it.
    /// </summary>
    public class DatabaseLifecycleTestSuite
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
        public DatabaseLifecycleTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// CreateDatabaseIfNotExistsAsync creates a new database (and is idempotent); InitializeTablesAsync creates the
        /// tables, ReadTables/ReadTablesAsync describe them, and AddMigrations registers and applies migrations there.
        /// </summary>
        [Fact]
        public async Task NewDatabaseLifecycle()
        {
            string name = "durable_life_" + Guid.NewGuid().ToString("N").Substring(0, 10);
            string connectionString = ConnectionStringFor(name);
            try
            {
                using (ISqlRepository<Product> repository = CreateRepository<Product>(connectionString))
                {
                    await repository.CreateDatabaseIfNotExistsAsync();
                    await repository.CreateDatabaseIfNotExistsAsync();
                    await repository.InitializeTablesAsync(new[] { typeof(Product), typeof(RelUpsertItem) });
                    SchemaValidationResult validation = await repository.ValidateTablesAsync(new[] { typeof(Product), typeof(RelUpsertItem) });
                    Assert.True(validation.IsValid, string.Join("; ", validation.Errors));

                    Product created = await repository.CreateAsync(new Product { Name = "lifecycle", Price = 1.5m, Sku = "L-1", Category = "c" });
                    Assert.True(created.Id > 0);
                    Assert.Equal(1L, await repository.CountAsync());
                }

                await using IConnectionFactory factory = CreateFactory(connectionString);
                DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
                Dictionary<string, TableSchema> tables = await reader.ReadTablesAsync(new[] { "products", "rel_upsert_items", "no_such_table" });
                Assert.Equal(2, tables.Count);
                Assert.NotNull(tables["products"].FindColumn("name"));
                Assert.NotNull(tables["rel_upsert_items"].FindColumn("code"));
                Dictionary<string, TableSchema> syncTables = reader.ReadTables(new[] { "products" });
                Assert.Single(syncTables);
                Assert.True(syncTables["products"].Columns.Count >= 4);

                SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect, new SqlMigratorOptions { HistoryTableName = "life_history" });
                Assert.Same(migrator, migrator.AddMigrations(new Migration[]
                {
                    new DelegateMigration("002_second", "second", ctx => ctx.ExecuteSqlRaw("UPDATE " + Q("products") + " SET " + Q("price") + " = 2")),
                    new DelegateMigration("001_first", "first", ctx => ctx.ExecuteSqlRaw("UPDATE " + Q("products") + " SET " + Q("name") + " = 'migrated'"))
                }));
                Assert.Equal(new[] { "001_first", "002_second" }, migrator.Migrations.Select(m => m.Id).ToArray());
                Assert.Throws<ArgumentException>(() => migrator.AddMigrations(new Migration[] { new DelegateMigration("001_first", null, ctx => { }) }));
                Assert.Throws<ArgumentNullException>(() => migrator.AddMigrations(null!));

                MigrationRunResult result = await migrator.MigrateAsync();
                Assert.Equal(2, result.Applied.Count);
                Assert.Empty(await migrator.GetPendingMigrationsAsync());
                using (ISqlRepository<Product> repository = CreateRepository<Product>(connectionString))
                {
                    Product migrated = (await repository.ReadFirstAsync())!;
                    Assert.Equal("migrated", migrated.Name);
                    Assert.Equal(2m, migrated.Price);
                }
            }
            finally
            {
                await DropDatabaseAsync(name, connectionString);
            }
        }

        #endregion

        #region Private-Methods

        private string Q(string identifier)
        {
            return _Provider.Dialect.QuoteIdentifier(identifier);
        }

        private string ConnectionStringFor(string database)
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite:
                    return "Data Source=" + Path.Combine(Path.GetTempPath(), database + ".db");
                case TestDatabaseType.MySql:
                case TestDatabaseType.MariaDb:
                    return new MySqlConnectionStringBuilder(_Provider.ConnectionString) { Database = database }.ConnectionString;
                case TestDatabaseType.Postgres:
                case TestDatabaseType.CockroachDb:
                case TestDatabaseType.YugabyteDb:
                    return new NpgsqlConnectionStringBuilder(_Provider.ConnectionString) { Database = database }.ConnectionString;
                case TestDatabaseType.SqlServer:
                    return new SqlConnectionStringBuilder(_Provider.ConnectionString) { InitialCatalog = database }.ConnectionString;
                default:
                    throw new NotSupportedException(_Provider.DatabaseType.ToString());
            }
        }

        private ISqlRepository<T> CreateRepository<T>(string connectionString) where T : class, new()
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite: return new SqliteRepository<T>(SqliteRepositorySettings.Parse(connectionString));
                case TestDatabaseType.MySql:
                case TestDatabaseType.MariaDb: return new MySqlRepository<T>(MySqlRepositorySettings.Parse(connectionString));
                case TestDatabaseType.Postgres:
                case TestDatabaseType.CockroachDb:
                case TestDatabaseType.YugabyteDb: return new PostgresRepository<T>(PostgresRepositorySettings.Parse(connectionString));
                case TestDatabaseType.SqlServer: return new SqlServerRepository<T>(SqlServerRepositorySettings.Parse(connectionString));
                default: throw new NotSupportedException(_Provider.DatabaseType.ToString());
            }
        }

        private IConnectionFactory CreateFactory(string connectionString)
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite: return new SqliteConnectionFactory(connectionString);
                case TestDatabaseType.MySql:
                case TestDatabaseType.MariaDb: return new MySqlConnectionFactory(connectionString);
                case TestDatabaseType.Postgres:
                case TestDatabaseType.CockroachDb:
                case TestDatabaseType.YugabyteDb: return new PostgresConnectionFactory(connectionString);
                case TestDatabaseType.SqlServer: return new SqlServerConnectionFactory(connectionString);
                default: throw new NotSupportedException(_Provider.DatabaseType.ToString());
            }
        }

        private async Task DropDatabaseAsync(string name, string connectionString)
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite:
                    SqliteConnection.ClearAllPools();
                    string path = new SqliteConnectionStringBuilder(connectionString).DataSource;
                    if (File.Exists(path)) File.Delete(path);
                    return;
                case TestDatabaseType.MySql:
                case TestDatabaseType.MariaDb:
                    MySqlConnection.ClearAllPools();
                    await ExecuteOnServerAsync("DROP DATABASE IF EXISTS " + Q(name));
                    return;
                case TestDatabaseType.Postgres:
                case TestDatabaseType.CockroachDb:
                case TestDatabaseType.YugabyteDb:
                    NpgsqlConnection.ClearAllPools();
                    await ExecuteOnServerAsync("DROP DATABASE IF EXISTS " + Q(name) + " WITH (FORCE)");
                    return;
                case TestDatabaseType.SqlServer:
                    SqlConnection.ClearAllPools();
                    await ExecuteOnServerAsync("IF DB_ID(N'" + name + "') IS NOT NULL BEGIN ALTER DATABASE " + Q(name) + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE " + Q(name) + "; END");
                    return;
            }
        }

        private async Task ExecuteOnServerAsync(string sql)
        {
            await using DbConnection connection = _Provider.CreateRawConnection();
            await connection.OpenAsync();
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        #endregion
    }
}
