namespace Durable.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// SQL Server repository for <typeparamref name="T"/>. All behavior comes from <see cref="SqlRepository{T}"/>; this class
    /// supplies the SQL Server dialect and connection handling, <see cref="SqlBulkCopy"/> bulk insert, and database creation.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class SqlServerRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Creates a repository from a connection string. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="connectionString">SqlClient connection string. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public SqlServerRepository(string connectionString, SqlRepositoryOptions? options = null)
            : this(SqlServerRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), connectionString, options)
        {
        }

        /// <summary>
        /// Creates a repository from settings. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        public SqlServerRepository(SqlServerRepositorySettings settings, SqlRepositoryOptions? options = null)
            : this(settings ?? throw new ArgumentNullException(nameof(settings)), settings.BuildConnectionString(), options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory. The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Factory producing <see cref="SqlConnection"/> instances. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        public SqlServerRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(SqlServerDialect.Default, connectionFactory, false, typeof(SqlConnection), null, options)
        {
        }

        private SqlServerRepository(SqlServerRepositorySettings settings, string connectionString, SqlRepositoryOptions? options)
            : base(SqlServerDialect.Default, new SqlServerConnectionFactory(connectionString), true, typeof(SqlConnection), settings, options)
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates the database named in the settings when it does not exist, connecting to master.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when no database name is configured.</exception>
        public override void CreateDatabaseIfNotExists()
        {
            ThrowIfDisposed();
            SqlConnectionStringBuilder builder = MasterConnection(out string database);
            using SqlConnection connection = new SqlConnection(builder.ToString());
            connection.Open();
            using SqlCommand command = CreateDatabaseCommand(connection, database);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Creates the database named in the settings when it does not exist.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no database name is configured.</exception>
        public override async Task CreateDatabaseIfNotExistsAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            SqlConnectionStringBuilder builder = MasterConnection(out string database);
            SqlConnection connection = new SqlConnection(builder.ToString());
            await using (connection.ConfigureAwait(false))
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
                SqlCommand command = CreateDatabaseCommand(connection, database);
                await using (command.ConfigureAwait(false))
                {
                    await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
            }
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Inserts rows with <see cref="SqlBulkCopy"/> inside the transaction.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <returns>Rows inserted.</returns>
        protected override long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            using SqlBulkCopy bulkCopy = CreateBulkCopy(lease);
            bulkCopy.WriteToServer(ToDataTable(entities));
            return entities.Count;
        }

        /// <summary>
        /// Inserts rows with <see cref="SqlBulkCopy"/> inside the transaction.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected override async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            using SqlBulkCopy bulkCopy = CreateBulkCopy(lease);
            await bulkCopy.WriteToServerAsync(ToDataTable(entities), token).ConfigureAwait(false);
            return entities.Count;
        }

        private SqlBulkCopy CreateBulkCopy(ConnectionLease lease)
        {
            SqlBulkCopy bulkCopy = new SqlBulkCopy((SqlConnection)lease.Connection, SqlBulkCopyOptions.Default, lease.Transaction as SqlTransaction)
            {
                DestinationTableName = Dialect.QuoteIdentifier(Metadata.TableName),
                BatchSize = 0
            };
            if (Options.CommandTimeoutSeconds.HasValue) bulkCopy.BulkCopyTimeout = Options.CommandTimeoutSeconds.Value;
            foreach (ColumnMetadata column in InsertColumns) bulkCopy.ColumnMappings.Add(column.Name, column.Name);
            return bulkCopy;
        }

        private DataTable ToDataTable(IReadOnlyList<T> entities)
        {
            DataTable table = new DataTable();
            foreach (ColumnMetadata column in InsertColumns) table.Columns.Add(column.Name, typeof(object));
            foreach (T entity in entities)
            {
                object[] values = new object[InsertColumns.Count];
                for (int i = 0; i < values.Length; i++) values[i] = DatabaseValue(entity, InsertColumns[i]);
                table.Rows.Add(values);
            }

            return table;
        }

        private SqlConnectionStringBuilder MasterConnection(out string database)
        {
            string connectionString = Settings?.BuildConnectionString()
                ?? throw new InvalidOperationException("CreateDatabaseIfNotExists requires a repository created from a connection string or settings.");
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
            if (string.IsNullOrEmpty(builder.InitialCatalog)) throw new InvalidOperationException("The connection string does not name a database.");
            database = builder.InitialCatalog;
            builder.InitialCatalog = "master";
            builder.Pooling = false;
            return builder;
        }

        private SqlCommand CreateDatabaseCommand(SqlConnection connection, string database)
        {
            SqlCommand command = new SqlCommand("IF DB_ID(@p0) IS NULL EXEC('CREATE DATABASE ' + @p1)", connection);
            command.Parameters.AddWithValue("@p0", database);
            command.Parameters.AddWithValue("@p1", Dialect.QuoteIdentifier(database));
            return command;
        }

        #endregion
    }
}
