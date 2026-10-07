namespace Durable.Oracle
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using global::Oracle.ManagedDataAccess.Client;

    /// <summary>
    /// Oracle repository for <typeparamref name="T"/>. All behavior comes from <see cref="SqlRepository{T}"/>; this class
    /// supplies the Oracle dialect and connection handling, and bulk insert through ODP.NET array binding (one INSERT
    /// executed for many rows per round trip).
    /// Thread safety: safe for concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class OracleRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Creates a repository from a connection string. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="connectionString">ODP.NET connection string. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public OracleRepository(string connectionString, SqlRepositoryOptions? options = null)
            : this(OracleRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), connectionString, options)
        {
        }

        /// <summary>
        /// Creates a repository from settings. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        public OracleRepository(OracleRepositorySettings settings, SqlRepositoryOptions? options = null)
            : this(settings ?? throw new ArgumentNullException(nameof(settings)), settings.BuildConnectionString(), options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory. The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Factory producing <see cref="OracleConnection"/> instances. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        public OracleRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(OracleDialect.Default, connectionFactory, false, typeof(OracleConnection), null, options)
        {
        }

        private OracleRepository(OracleRepositorySettings settings, string connectionString, SqlRepositoryOptions? options)
            : base(OracleDialect.Default, new OracleConnectionFactory(connectionString), true, typeof(OracleConnection), settings, options)
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Verifies that the configured database can be reached. Oracle databases (pluggable databases) and schemas
        /// (users) are created by a DBA, so nothing is created; this keeps the call portable across providers.
        /// </summary>
        /// <exception cref="OracleException">Thrown when the database cannot be reached.</exception>
        public override void CreateDatabaseIfNotExists()
        {
            ThrowIfDisposed();
            using DbConnection connection = ConnectionFactory.OpenConnection();
        }

        /// <summary>
        /// Verifies that the configured database can be reached. Oracle databases and schemas are created by a DBA, so
        /// nothing is created.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="OracleException">Thrown when the database cannot be reached.</exception>
        public override async Task CreateDatabaseIfNotExistsAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            DbConnection connection = await ConnectionFactory.OpenConnectionAsync(token).ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Inserts rows with ODP.NET array binding inside the transaction.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <returns>Rows inserted.</returns>
        protected override long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            long total = 0;
            foreach (List<T> chunk in Chunks(entities))
            {
                using DbCommand command = CreateArrayBoundCommand(lease, chunk);
                command.ExecuteNonQuery();
                total += chunk.Count;
            }

            return total;
        }

        /// <summary>
        /// Inserts rows with ODP.NET array binding inside the transaction.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected override async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            long total = 0;
            foreach (List<T> chunk in Chunks(entities))
            {
                token.ThrowIfCancellationRequested();
                DbCommand command = CreateArrayBoundCommand(lease, chunk);
                await using (command.ConfigureAwait(false))
                {
                    await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    total += chunk.Count;
                }
            }

            return total;
        }

        private IEnumerable<List<T>> Chunks(IReadOnlyList<T> entities)
        {
            int size = Math.Max(1, Options.BatchConfiguration.MaxRowsPerBatch);
            for (int offset = 0; offset < entities.Count; offset += size)
            {
                List<T> chunk = new List<T>(Math.Min(size, entities.Count - offset));
                for (int i = offset; i < offset + size && i < entities.Count; i++) chunk.Add(entities[i]);
                yield return chunk;
            }
        }

        private DbCommand CreateArrayBoundCommand(ConnectionLease lease, List<T> rows)
        {
            OracleDialect dialect = (OracleDialect)Dialect;
            List<string> placeholders = new List<string>(InsertColumns.Count);
            for (int i = 0; i < InsertColumns.Count; i++) placeholders.Add(dialect.FormatParameterName(i));

            string sql = InsertColumns.Count == 0
                ? "INSERT INTO " + dialect.QuoteIdentifier(Metadata.TableName) + " " + dialect.InsertDefaultValuesClause
                : "INSERT INTO " + dialect.QuoteIdentifier(Metadata.TableName) + " (" + string.Join(", ", InsertColumns.Select(c => dialect.QuoteIdentifier(c.Name)))
                    + ") VALUES (" + string.Join(", ", placeholders) + ")";

            DbCommand command = CreateCommand(lease, new SqlStatement(sql));
            OracleCommand oracleCommand = (OracleCommand)command;
            oracleCommand.ArrayBindCount = rows.Count;
            for (int c = 0; c < InsertColumns.Count; c++)
            {
                ColumnMetadata column = InsertColumns[c];
                object[] values = new object[rows.Count];
                for (int r = 0; r < rows.Count; r++) values[r] = DatabaseValue(rows[r], column);
                OracleParameter parameter = new OracleParameter(placeholders[c], ArrayBindType(dialect, column, values))
                {
                    Direction = ParameterDirection.Input,
                    Value = values
                };
                oracleCommand.Parameters.Add(parameter);
            }

            return command;
        }

        private static OracleDbType ArrayBindType(OracleDialect dialect, ColumnMetadata column, object[] values)
        {
            string columnType = dialect.GetColumnType(column);
            if (string.Equals(columnType, "CLOB", StringComparison.OrdinalIgnoreCase)) return OracleDbType.Clob;
            if (string.Equals(columnType, "BLOB", StringComparison.OrdinalIgnoreCase)) return OracleDbType.Blob;
            object? sample = values.FirstOrDefault(v => v != null && v != DBNull.Value);
            switch (sample)
            {
                case short: return OracleDbType.Int16;
                case int: return OracleDbType.Int32;
                case long: return OracleDbType.Int64;
                case decimal: return OracleDbType.Decimal;
                case float: return OracleDbType.BinaryFloat;
                case double: return OracleDbType.BinaryDouble;
                case DateTime: return OracleDbType.TimeStamp;
                case DateTimeOffset: return OracleDbType.TimeStampTZ;
                case TimeSpan: return OracleDbType.IntervalDS;
                case byte[]: return values.Any(v => v is byte[] bytes && bytes.Length > 2000) ? OracleDbType.Blob : OracleDbType.Raw;
                case string: return values.Any(v => v is string text && text.Length > 2000) ? OracleDbType.Clob : OracleDbType.Varchar2;
                case null: return OracleDbType.Varchar2;
                default: return OracleDbType.Varchar2;
            }
        }

        #endregion
    }
}
