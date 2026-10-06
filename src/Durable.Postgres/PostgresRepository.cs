namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using NpgsqlTypes;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// PostgreSQL repository for <typeparamref name="T"/>. All behavior comes from <see cref="SqlRepository{T}"/>; this class
    /// supplies the PostgreSQL dialect, an <see cref="NpgsqlDataSource"/>-based connection factory, binary COPY bulk insert,
    /// and database creation.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class PostgresRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Creates a repository from a connection string. The repository owns its connection factory and disposes it.
        /// Prefer sharing one <see cref="PostgresConnectionFactory"/> across repositories in long-running applications.
        /// </summary>
        /// <param name="connectionString">Npgsql connection string. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public PostgresRepository(string connectionString, SqlRepositoryOptions? options = null)
            : this(PostgresRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), connectionString, options)
        {
        }

        /// <summary>
        /// Creates a repository from settings. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        public PostgresRepository(PostgresRepositorySettings settings, SqlRepositoryOptions? options = null)
            : this(settings ?? throw new ArgumentNullException(nameof(settings)), settings.BuildConnectionString(), options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory. The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Factory producing <see cref="NpgsqlConnection"/> instances. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        public PostgresRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(PostgresDialect.Default, connectionFactory, false, typeof(NpgsqlConnection), null, options)
        {
        }

        private PostgresRepository(PostgresRepositorySettings settings, string connectionString, SqlRepositoryOptions? options)
            : base(PostgresDialect.Default, new PostgresConnectionFactory(connectionString), true, typeof(NpgsqlConnection), settings, options)
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates the database named in the settings when it does not exist, connecting to the "postgres" maintenance database.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when no database name is configured.</exception>
        public override void CreateDatabaseIfNotExists()
        {
            ThrowIfDisposed();
            NpgsqlConnectionStringBuilder builder = MaintenanceConnection(out string database);
            using NpgsqlConnection connection = new NpgsqlConnection(builder.ToString());
            connection.Open();
            using (NpgsqlCommand exists = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = @p0", connection))
            {
                exists.Parameters.AddWithValue("@p0", database);
                if (exists.ExecuteScalar() != null) return;
            }

            using NpgsqlCommand create = new NpgsqlCommand("CREATE DATABASE " + Dialect.QuoteIdentifier(database), connection);
            create.ExecuteNonQuery();
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
            NpgsqlConnectionStringBuilder builder = MaintenanceConnection(out string database);
            NpgsqlConnection connection = new NpgsqlConnection(builder.ToString());
            await using (connection.ConfigureAwait(false))
            {
                await connection.OpenAsync(token).ConfigureAwait(false);
                NpgsqlCommand exists = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = @p0", connection);
                await using (exists.ConfigureAwait(false))
                {
                    exists.Parameters.AddWithValue("@p0", database);
                    if (await exists.ExecuteScalarAsync(token).ConfigureAwait(false) != null) return;
                }

                NpgsqlCommand create = new NpgsqlCommand("CREATE DATABASE " + Dialect.QuoteIdentifier(database), connection);
                await using (create.ConfigureAwait(false))
                {
                    await create.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
            }
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Inserts rows with PostgreSQL binary COPY. Column CLR types must match the table's column types.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <returns>Rows inserted.</returns>
        protected override long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            NpgsqlConnection connection = (NpgsqlConnection)lease.Connection;
            using NpgsqlBinaryImporter importer = connection.BeginBinaryImport(CopyCommand());
            foreach (T entity in entities)
            {
                importer.StartRow();
                foreach (ColumnMetadata column in InsertColumns)
                {
                    object value = DatabaseValue(entity, column);
                    if (value == DBNull.Value) importer.WriteNull();
                    else importer.Write(value, NpgsqlTypeFor(column, value));
                }
            }

            return (long)importer.Complete();
        }

        /// <summary>
        /// Inserts rows with PostgreSQL binary COPY.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected override async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            NpgsqlConnection connection = (NpgsqlConnection)lease.Connection;
            NpgsqlBinaryImporter importer = await connection.BeginBinaryImportAsync(CopyCommand(), token).ConfigureAwait(false);
            await using (importer.ConfigureAwait(false))
            {
                foreach (T entity in entities)
                {
                    await importer.StartRowAsync(token).ConfigureAwait(false);
                    foreach (ColumnMetadata column in InsertColumns)
                    {
                        object value = DatabaseValue(entity, column);
                        if (value == DBNull.Value) await importer.WriteNullAsync(token).ConfigureAwait(false);
                        else await importer.WriteAsync(value, NpgsqlTypeFor(column, value), token).ConfigureAwait(false);
                    }
                }

                return (long)await importer.CompleteAsync(token).ConfigureAwait(false);
            }
        }

        private NpgsqlConnectionStringBuilder MaintenanceConnection(out string database)
        {
            string connectionString = Settings?.BuildConnectionString()
                ?? throw new InvalidOperationException("CreateDatabaseIfNotExists requires a repository created from a connection string or settings.");
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder(connectionString);
            database = builder.Database ?? throw new InvalidOperationException("The connection string does not name a database.");
            builder.Database = "postgres";
            builder.Pooling = false;
            return builder;
        }

        private string CopyCommand()
        {
            List<string> columns = new List<string>(InsertColumns.Count);
            foreach (ColumnMetadata column in InsertColumns) columns.Add(Dialect.QuoteIdentifier(column.Name));
            return "COPY " + Dialect.QuoteIdentifier(Metadata.TableName) + " (" + string.Join(", ", columns) + ") FROM STDIN (FORMAT BINARY)";
        }

        private static NpgsqlDbType NpgsqlTypeFor(ColumnMetadata column, object value)
        {
            if (column.IsJson && column.Converter == null) return NpgsqlDbType.Jsonb;
            switch (value)
            {
                case bool: return NpgsqlDbType.Boolean;
                case short: return NpgsqlDbType.Smallint;
                case int: return NpgsqlDbType.Integer;
                case long: return NpgsqlDbType.Bigint;
                case float: return NpgsqlDbType.Real;
                case double: return NpgsqlDbType.Double;
                case decimal: return NpgsqlDbType.Numeric;
                case DateTime: return NpgsqlDbType.Timestamp;
                case DateTimeOffset: return NpgsqlDbType.TimestampTz;
                case DateOnly: return NpgsqlDbType.Date;
                case TimeOnly: return NpgsqlDbType.Time;
                case TimeSpan: return NpgsqlDbType.Interval;
                case Guid: return NpgsqlDbType.Uuid;
                case byte[]: return NpgsqlDbType.Bytea;
                default: return NpgsqlDbType.Text;
            }
        }

        #endregion
    }
}
