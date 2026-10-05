namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// SQLite repository for <typeparamref name="T"/>. All behavior comes from <see cref="SqlRepository{T}"/>; this class
    /// supplies the SQLite dialect and connection handling and a prepared-statement bulk insert.
    /// Thread safety: safe for concurrent use (SQLite itself serializes writers).
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class SqliteRepository<T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Creates a repository from a connection string. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="connectionString">SQLite connection string. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public SqliteRepository(string connectionString, SqlRepositoryOptions? options = null)
            : this(SqliteRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), connectionString, options)
        {
        }

        /// <summary>
        /// Creates a repository from settings. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        public SqliteRepository(SqliteRepositorySettings settings, SqlRepositoryOptions? options = null)
            : this(settings ?? throw new ArgumentNullException(nameof(settings)), settings.BuildConnectionString(), options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory. The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Connection factory producing <see cref="SqliteConnection"/> instances. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        public SqliteRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(SqliteDialect.Default, connectionFactory, false, typeof(SqliteConnection), null, options)
        {
        }

        private SqliteRepository(SqliteRepositorySettings settings, string connectionString, SqlRepositoryOptions? options)
            : base(SqliteDialect.Default, new SqliteConnectionFactory(connectionString), true, typeof(SqliteConnection), settings, options)
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Ensures the database file exists by opening a connection (in-memory databases need no action).
        /// </summary>
        public override void CreateDatabaseIfNotExists()
        {
            ThrowIfDisposed();
            if (Settings is SqliteRepositorySettings settings && !string.IsNullOrEmpty(settings.DataSource) && settings.Mode != SqliteOpenMode.Memory && settings.DataSource != ":memory:")
            {
                string? directory = Path.GetDirectoryName(Path.GetFullPath(settings.DataSource));
                if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            }

            using DbConnection connection = ConnectionFactory.OpenConnection();
        }

        /// <summary>
        /// Ensures the database file exists by opening a connection.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        public override async Task CreateDatabaseIfNotExistsAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            if (Settings is SqliteRepositorySettings settings && !string.IsNullOrEmpty(settings.DataSource) && settings.Mode != SqliteOpenMode.Memory && settings.DataSource != ":memory:")
            {
                string? directory = Path.GetDirectoryName(Path.GetFullPath(settings.DataSource));
                if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            }

            DbConnection connection = await ConnectionFactory.OpenConnectionAsync(token).ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Inserts rows with a single prepared INSERT executed once per row inside the transaction, the fastest approach for SQLite.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <returns>Rows inserted.</returns>
        protected override long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            using DbCommand command = CreatePreparedInsert(lease);
            long total = 0;
            foreach (T entity in entities)
            {
                BindRow(command, entity);
                total += command.ExecuteNonQuery();
            }

            return total;
        }

        /// <summary>
        /// Inserts rows with a single prepared INSERT executed once per row inside the transaction.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected override async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            DbCommand command = CreatePreparedInsert(lease);
            await using (command.ConfigureAwait(false))
            {
                long total = 0;
                foreach (T entity in entities)
                {
                    token.ThrowIfCancellationRequested();
                    BindRow(command, entity);
                    total += await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }

                return total;
            }
        }

        private DbCommand CreatePreparedInsert(ConnectionLease lease)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            builder.Append("INSERT INTO ").AppendIdentifier(Metadata.TableName).Append(" (");
            for (int i = 0; i < InsertColumns.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.AppendIdentifier(InsertColumns[i].Name);
            }

            builder.Append(") VALUES (");
            for (int i = 0; i < InsertColumns.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.AppendParameter(DBNull.Value, InsertColumns[i]);
            }

            builder.Append(")");
            DbCommand command = Executor.CreateCommand(lease, builder.Build());
            command.Prepare();
            return command;
        }

        private void BindRow(DbCommand command, T entity)
        {
            for (int i = 0; i < InsertColumns.Count; i++) command.Parameters[i].Value = DatabaseValue(entity, InsertColumns[i]);
        }

        #endregion
    }
}
