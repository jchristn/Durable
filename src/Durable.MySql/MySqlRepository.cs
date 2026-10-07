namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// MySQL repository for <typeparamref name="T"/>. All behavior comes from <see cref="SqlRepository{T}"/>; this class
    /// supplies the MySQL dialect, a <see cref="MySqlDataSource"/>-based connection factory, bulk insert through
    /// <see cref="MySqlBulkCopy"/> (when the connection string sets AllowLoadLocalInfile=true and the server allows it;
    /// otherwise batched multi-row INSERT), and database creation.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class MySqlRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Creates a repository from a connection string. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="connectionString">MySqlConnector connection string. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public MySqlRepository(string connectionString, SqlRepositoryOptions? options = null)
            : this(MySqlDialect.Default, MySqlRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), connectionString, MySqlFlavor.MySql, options)
        {
        }

        /// <summary>
        /// Creates a repository from a connection string for a MySQL-compatible database. The repository owns its
        /// connection factory and disposes it.
        /// </summary>
        /// <param name="connectionString">MySqlConnector connection string. Must not be null.</param>
        /// <param name="flavor">Database flavor; selects the dialect (<see cref="MySqlDialect.For(MySqlFlavor)"/>).</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when flavor is not a defined value.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public MySqlRepository(string connectionString, MySqlFlavor flavor, SqlRepositoryOptions? options = null)
            : this(MySqlDialect.For(flavor), MySqlRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString)), flavor), connectionString, flavor, options)
        {
        }

        /// <summary>
        /// Creates a repository from settings. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        public MySqlRepository(MySqlRepositorySettings settings, SqlRepositoryOptions? options = null)
            : this(MySqlDialect.For((settings ?? throw new ArgumentNullException(nameof(settings))).Flavor), settings, settings.BuildConnectionString(), settings.Flavor, options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory. The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Factory producing <see cref="MySqlConnection"/> instances. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        public MySqlRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(DialectFor(connectionFactory), connectionFactory, false, typeof(MySqlConnection), null, options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory with an explicit dialect (for example a flavor's dialect, or
        /// a <see cref="MySqlDialect"/> with a custom ordinal collation). The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Factory producing <see cref="MySqlConnection"/> instances. Must not be null.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory or dialect is null.</exception>
        public MySqlRepository(IConnectionFactory connectionFactory, MySqlDialect dialect, SqlRepositoryOptions? options = null)
            : base(dialect ?? throw new ArgumentNullException(nameof(dialect)), connectionFactory, false, typeof(MySqlConnection), null, options)
        {
        }

        private MySqlRepository(MySqlDialect dialect, MySqlRepositorySettings settings, string connectionString, MySqlFlavor flavor, SqlRepositoryOptions? options)
            : base(dialect, new MySqlConnectionFactory(connectionString) { Flavor = flavor }, true, typeof(MySqlConnection), settings, options)
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates the database named in the settings when it does not exist.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when no database name is configured.</exception>
        public override void CreateDatabaseIfNotExists()
        {
            ThrowIfDisposed();
            MySqlConnectionStringBuilder builder = ServerConnection(out string database);
            using MySqlConnection connection = new MySqlConnection(builder.ToString());
            connection.Open();
            using MySqlCommand command = new MySqlCommand("CREATE DATABASE IF NOT EXISTS " + Dialect.QuoteIdentifier(database), connection);
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
            MySqlConnectionStringBuilder builder = ServerConnection(out string database);
            MySqlConnection connection = new MySqlConnection(builder.ToString());
            await using (connection.ConfigureAwait(false))
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
                MySqlCommand command = new MySqlCommand("CREATE DATABASE IF NOT EXISTS " + Dialect.QuoteIdentifier(database), connection);
                await using (command.ConfigureAwait(false))
                {
                    await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
            }
        }

        #endregion

        #region Private-Methods

        private static MySqlDialect DialectFor(IConnectionFactory connectionFactory)
        {
            return connectionFactory is MySqlConnectionFactory typed ? MySqlDialect.For(typed.Flavor) : MySqlDialect.Default;
        }

        /// <summary>
        /// Inserts rows with <see cref="MySqlBulkCopy"/> when local infile is allowed; otherwise uses batched multi-row INSERT.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <returns>Rows inserted.</returns>
        protected override long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            if (!AllowsLocalInfile(lease)) return base.BulkInsertCore(lease, entities);
            MySqlBulkCopy bulkCopy = CreateBulkCopy(lease);
            MySqlBulkCopyResult result = bulkCopy.WriteToServer(ToDataTable(entities));
            return result.RowsInserted;
        }

        /// <summary>
        /// Inserts rows with <see cref="MySqlBulkCopy"/> when local infile is allowed; otherwise uses batched multi-row INSERT.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected override async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            if (!AllowsLocalInfile(lease)) return await base.BulkInsertCoreAsync(lease, entities, token).ConfigureAwait(false);
            MySqlBulkCopy bulkCopy = CreateBulkCopy(lease);
            MySqlBulkCopyResult result = await bulkCopy.WriteToServerAsync(ToDataTable(entities), token).ConfigureAwait(false);
            return result.RowsInserted;
        }

        private static bool AllowsLocalInfile(ConnectionLease lease)
        {
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder(lease.Connection.ConnectionString);
            return builder.AllowLoadLocalInfile;
        }

        private MySqlBulkCopy CreateBulkCopy(ConnectionLease lease)
        {
            MySqlBulkCopy bulkCopy = new MySqlBulkCopy((MySqlConnection)lease.Connection, lease.Transaction as MySqlTransaction)
            {
                DestinationTableName = Dialect.QuoteIdentifier(Metadata.TableName)
            };
            for (int i = 0; i < InsertColumns.Count; i++)
                bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(i, InsertColumns[i].Name));
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

        private MySqlConnectionStringBuilder ServerConnection(out string database)
        {
            string connectionString = Settings?.BuildConnectionString()
                ?? throw new InvalidOperationException("CreateDatabaseIfNotExists requires a repository created from a connection string or settings.");
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder(connectionString);
            if (string.IsNullOrEmpty(builder.Database)) throw new InvalidOperationException("The connection string does not name a database.");
            database = builder.Database;
            builder.Database = string.Empty;
            builder.Pooling = false;
            return builder;
        }

        #endregion
    }
}
