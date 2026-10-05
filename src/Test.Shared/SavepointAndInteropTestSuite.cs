namespace Test.Shared
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Npgsql;
    using Xunit;

    /// <summary>
    /// Coverage for savepoints on <see cref="ISqlTransaction"/>, interop with connections and transactions opened
    /// by the caller through <see cref="SqlTransactionContext.Wrap"/>, and rejection of transactions that belong to
    /// a different database provider. Executed identically across all database providers.
    /// </summary>
    public class SavepointAndInteropTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SavepointAndInteropTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public SavepointAndInteropTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Rolling back to a savepoint undoes only the work done after it; earlier and later work commits.
        /// </summary>
        [Fact]
        public async Task Savepoint_RollbackUndoesOnlyLaterWork()
        {
            const string department = "SpRollback";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (ISqlTransaction transaction = repository.BeginTransaction())
            {
                repository.Create(InfrastructureTestData.NewPerson("sp-a@example.com", department), transaction);
                ISavepoint savepoint = transaction.CreateSavepoint("sp_before_b");
                Assert.Equal("sp_before_b", savepoint.Name);
                repository.Create(InfrastructureTestData.NewPerson("sp-b@example.com", department), transaction);
                Assert.Equal(2, repository.Count(p => p.Department == department, transaction));

                await savepoint.RollbackAsync();
                Assert.Equal(1, repository.Count(p => p.Department == department, transaction));

                repository.Create(InfrastructureTestData.NewPerson("sp-c@example.com", department), transaction);
                transaction.Commit();
            }

            Assert.Equal(1, await repository.CountAsync(p => p.Email == "sp-a@example.com"));
            Assert.Equal(0, await repository.CountAsync(p => p.Email == "sp-b@example.com"));
            Assert.Equal(1, await repository.CountAsync(p => p.Email == "sp-c@example.com"));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// The synchronous savepoint rollback path, with an auto-generated name, also undoes later work.
        /// </summary>
        [Fact]
        public async Task Savepoint_SyncRollbackWithGeneratedName()
        {
            const string department = "SpRollbackSync";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            ISqlTransaction transaction = await repository.BeginTransactionAsync();
            try
            {
                await repository.CreateAsync(InfrastructureTestData.NewPerson("sps-a@example.com", department), transaction);
                ISavepoint savepoint = await transaction.CreateSavepointAsync();
                Assert.False(string.IsNullOrWhiteSpace(savepoint.Name));
                await repository.CreateAsync(InfrastructureTestData.NewPerson("sps-b@example.com", department), transaction);
                savepoint.Rollback();
                await transaction.CommitAsync();
            }
            finally
            {
                await transaction.DisposeAsync();
            }

            Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
            Assert.Equal(1, await repository.CountAsync(p => p.Email == "sps-a@example.com"));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Releasing a savepoint keeps its work, which then commits with the transaction. On SQL Server release is a no-op.
        /// </summary>
        [Fact]
        public async Task Savepoint_ReleaseKeepsWork()
        {
            const string department = "SpRelease";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (ISqlTransaction transaction = repository.BeginTransaction())
            {
                ISavepoint first = transaction.CreateSavepoint("sp_first");
                repository.Create(InfrastructureTestData.NewPerson("spr-a@example.com", department), transaction);
                first.Release();

                ISavepoint second = await transaction.CreateSavepointAsync("sp_second");
                repository.Create(InfrastructureTestData.NewPerson("spr-b@example.com", department), transaction);
                await second.ReleaseAsync();

                transaction.Commit();
            }

            Assert.Equal(2, await repository.CountAsync(p => p.Department == department));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Rolling back the whole transaction after a released savepoint discards the savepoint's work too.
        /// </summary>
        [Fact]
        public async Task Savepoint_OuterRollbackDiscardsReleasedWork()
        {
            const string department = "SpOuterRollback";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (ISqlTransaction transaction = repository.BeginTransaction())
            {
                ISavepoint savepoint = transaction.CreateSavepoint();
                repository.Create(InfrastructureTestData.NewPerson("spo-a@example.com", department), transaction);
                savepoint.Release();
                transaction.Rollback();
            }

            Assert.Equal(0, await repository.CountAsync(p => p.Department == department));
        }

        /// <summary>
        /// Savepoint names containing anything other than letters, digits and underscores are rejected.
        /// </summary>
        [Fact]
        public void Savepoint_InvalidNameThrows()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            using ISqlTransaction transaction = repository.BeginTransaction();
            Assert.Throws<ArgumentException>(() => transaction.CreateSavepoint("bad name; DROP"));
            transaction.Rollback();
        }

        /// <summary>
        /// Savepoints on a wrapped external transaction created without a dialect use the driver's savepoint API.
        /// </summary>
        [Fact]
        public async Task Savepoint_DriverSavepointOnWrappedTransaction()
        {
            const string department = "SpDriver";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            await using (DbConnection connection = _Provider.CreateRawConnection())
            {
                await connection.OpenAsync();
                await using (DbTransaction dbTransaction = await connection.BeginTransactionAsync())
                {
                    SqlTransactionContext context = SqlTransactionContext.Wrap(connection, dbTransaction);
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("spd-a@example.com", department), context);
                    ISavepoint savepoint = context.CreateSavepoint("sp_driver");
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("spd-b@example.com", department), context);
                    savepoint.Rollback();
                    Assert.Equal(1, await repository.CountAsync(p => p.Department == department, context));
                    await dbTransaction.CommitAsync();
                }
            }

            Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
            Assert.Equal(1, await repository.CountAsync(p => p.Email == "spd-a@example.com"));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// A raw ADO.NET write and a Durable write through <see cref="SqlTransactionContext.Wrap"/> share one
        /// external transaction: both are visible inside, and both disappear when the caller rolls back.
        /// Durable must not close the caller's connection.
        /// </summary>
        [Fact]
        public async Task ExternalTransaction_RollbackDiscardsBothWrites()
        {
            const string department = "ExtTxRollback";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            await using (DbConnection connection = _Provider.CreateRawConnection())
            {
                await connection.OpenAsync();
                await using (DbTransaction dbTransaction = await connection.BeginTransactionAsync())
                {
                    await InsertRawAsync(connection, dbTransaction, "ext-raw-rb@example.com", department);

                    using (SqlTransactionContext context = SqlTransactionContext.Wrap(connection, dbTransaction, _Provider.Dialect))
                    {
                        Assert.False(context.OwnsConnection);
                        await repository.CreateAsync(InfrastructureTestData.NewPerson("ext-durable-rb@example.com", department), context);
                        repository.Create(InfrastructureTestData.NewPerson("ext-durable-rb-sync@example.com", department), context);
                        Assert.Equal(3, await repository.CountAsync(p => p.Department == department, context));
                        Assert.Equal(ConnectionState.Open, connection.State);
                    }

                    Assert.Equal(ConnectionState.Open, connection.State);
                    Assert.Equal(3L, await CountRawAsync(connection, dbTransaction, department));
                    await dbTransaction.RollbackAsync();
                }

                Assert.Equal(ConnectionState.Open, connection.State);
            }

            Assert.Equal(0, await repository.CountAsync(p => p.Department == department));
        }

        /// <summary>
        /// A raw ADO.NET write and a Durable write through <see cref="SqlTransactionContext.Wrap"/> both persist when
        /// the caller commits the external transaction, and the caller's connection remains open and usable.
        /// </summary>
        [Fact]
        public async Task ExternalTransaction_CommitPersistsBothWrites()
        {
            const string department = "ExtTxCommit";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            await using (DbConnection connection = _Provider.CreateRawConnection())
            {
                await connection.OpenAsync();
                await using (DbTransaction dbTransaction = await connection.BeginTransactionAsync())
                {
                    await InsertRawAsync(connection, dbTransaction, "ext-raw-c@example.com", department);
                    SqlTransactionContext context = SqlTransactionContext.Wrap(connection, dbTransaction, _Provider.Dialect);
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("ext-durable-c@example.com", department), context);
                    await context.DisposeAsync();
                    Assert.Equal(ConnectionState.Open, connection.State);
                    await dbTransaction.CommitAsync();
                }

                Assert.Equal(ConnectionState.Open, connection.State);
                Assert.Equal(2L, await CountRawAsync(connection, null, department));
            }

            Assert.Equal(1, await repository.CountAsync(p => p.Email == "ext-raw-c@example.com"));
            Assert.Equal(1, await repository.CountAsync(p => p.Email == "ext-durable-c@example.com"));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// A wrapped external connection without a transaction can be used for Durable operations and stays open.
        /// </summary>
        [Fact]
        public async Task ExternalConnectionWithoutTransaction_StaysOpen()
        {
            const string department = "ExtConnNoTx";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            await using (DbConnection connection = _Provider.CreateRawConnection())
            {
                await connection.OpenAsync();
                SqlTransactionContext context = SqlTransactionContext.Wrap(connection, null, _Provider.Dialect);
                await repository.CreateAsync(InfrastructureTestData.NewPerson("ext-notx@example.com", department), context);
                Assert.Equal(1, await repository.CountAsync(p => p.Department == department, context));
                context.Dispose();
                Assert.Equal(ConnectionState.Open, connection.State);
            }

            Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Wrapping a transaction that belongs to a different connection is rejected.
        /// </summary>
        [Fact]
        public async Task Wrap_TransactionFromOtherConnectionThrows()
        {
            await using DbConnection first = _Provider.CreateRawConnection();
            await using DbConnection second = _Provider.CreateRawConnection();
            await first.OpenAsync();
            await second.OpenAsync();
            await using DbTransaction transaction = await first.BeginTransactionAsync();
            Assert.Throws<ArgumentException>(() => SqlTransactionContext.Wrap(second, transaction, _Provider.Dialect));
            await transaction.RollbackAsync();
        }

        /// <summary>
        /// Passing a transaction whose connection belongs to a different provider throws <see cref="ArgumentException"/>
        /// and writes nothing.
        /// </summary>
        [Fact]
        public async Task CrossProviderTransaction_ThrowsArgumentException()
        {
            const string department = "CrossProvider";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            DbConnection foreign;
            ISqlDialect foreignDialect;
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite)
            {
                foreign = new NpgsqlConnection("Host=127.0.0.1;Database=unused");
                foreignDialect = PostgresDialect.Default;
            }
            else
            {
                foreign = new SqliteConnection("Data Source=:memory:");
                foreignDialect = SqliteDialect.Default;
            }

            await using (foreign)
            {
                SqlTransactionContext context = SqlTransactionContext.Wrap(foreign, null, foreignDialect);
                await Assert.ThrowsAsync<ArgumentException>(() =>
                    repository.CreateAsync(InfrastructureTestData.NewPerson("cross@example.com", department), context));
                Assert.Throws<ArgumentException>(() => repository.Count(null, context));
                Assert.Throws<ArgumentException>(() => repository.ExecuteSql("SELECT 1", context));
            }

            Assert.Equal(0, await repository.CountAsync(p => p.Department == department));
        }

        /// <summary>
        /// A real open SQLite transaction passed to a repository of another provider is rejected (skipped on SQLite).
        /// </summary>
        [Fact]
        public async Task CrossProviderOpenSqliteTransaction_ThrowsArgumentException()
        {
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite) return;

            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            using (SqliteRepository<Person> sqlite = new SqliteRepository<Person>("Data Source=:memory:"))
            {
                using ISqlTransaction sqliteTransaction = await sqlite.BeginTransactionAsync();
                await Assert.ThrowsAsync<ArgumentException>(() => repository.CountAsync(null, sqliteTransaction));
                await Assert.ThrowsAsync<ArgumentException>(() =>
                    repository.CreateAsync(InfrastructureTestData.NewPerson("cross-open@example.com", "CrossProvider"), sqliteTransaction));
                sqliteTransaction.Rollback();
            }
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private static async Task InsertRawAsync(DbConnection connection, DbTransaction transaction, string email, string department)
        {
            await using DbCommand command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO people (first, last, age, email, salary, department) VALUES (@first, @last, @age, @email, @salary, @department)";
            AddParameter(command, "@first", "Raw");
            AddParameter(command, "@last", "Ado");
            AddParameter(command, "@age", 33);
            AddParameter(command, "@email", email);
            AddParameter(command, "@salary", 1000m);
            AddParameter(command, "@department", department);
            int rows = await command.ExecuteNonQueryAsync();
            Assert.Equal(1, rows);
        }

        private static async Task<long> CountRawAsync(DbConnection connection, DbTransaction? transaction, string department)
        {
            await using DbCommand command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT COUNT(*) FROM people WHERE department = @department";
            AddParameter(command, "@department", department);
            object? value = await command.ExecuteScalarAsync();
            return Convert.ToInt64(value);
        }

        private static void AddParameter(DbCommand command, string name, object value)
        {
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            command.Parameters.Add(parameter);
        }

        #endregion
    }
}
