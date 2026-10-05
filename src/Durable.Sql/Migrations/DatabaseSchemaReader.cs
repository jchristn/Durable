namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Reads the live schema of tables (columns with type, nullability, max length and key membership; secondary indexes)
    /// through the dialect's introspection queries (<see cref="ISqlDialect.ColumnSchemaQuery"/>,
    /// <see cref="ISqlDialect.IndexSchemaQuery"/>). Tables are resolved in the connection's default schema.
    /// Thread safety: safe for concurrent use; each call opens its own connection.
    /// </summary>
    public class DatabaseSchemaReader
    {
        #region Public-Members

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets the connection factory. Never null.
        /// </summary>
        public IConnectionFactory ConnectionFactory { get; }

        #endregion

        #region Private-Members

        private readonly SqlCommandExecutor _Executor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a schema reader. The reader never disposes the connection factory.
        /// </summary>
        /// <param name="connectionFactory">Connection factory. Must not be null.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="options">Command options (timeout, interceptors, logging); null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory or dialect is null.</exception>
        public DatabaseSchemaReader(IConnectionFactory connectionFactory, ISqlDialect dialect, SqlRepositoryOptions? options = null)
        {
            ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _Executor = CreateExecutor(dialect, connectionFactory, options, "schema", null);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns whether a table exists.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <returns>True when the table exists.</returns>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        public bool TableExists(string tableName)
        {
            RequireName(tableName);
            using MigrationSession session = MigrationSession.Open(_Executor);
            return TableExists(session, tableName);
        }

        /// <summary>
        /// Returns whether a table exists.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True when the table exists.</returns>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        public async Task<bool> TableExistsAsync(string tableName, CancellationToken token = default)
        {
            RequireName(tableName);
            token.ThrowIfCancellationRequested();
            MigrationSession session = await MigrationSession.OpenAsync(_Executor, token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                return await TableExistsAsync(session, tableName, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reads a table's columns and secondary indexes.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <returns>The table, or null when it does not exist.</returns>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        /// <exception cref="NotSupportedException">Thrown when the dialect does not support introspection.</exception>
        public TableSchema? ReadTable(string tableName)
        {
            RequireName(tableName);
            using MigrationSession session = MigrationSession.Open(_Executor);
            return ReadTable(session, tableName);
        }

        /// <summary>
        /// Reads a table's columns and secondary indexes.
        /// </summary>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The table, or null when it does not exist.</returns>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        /// <exception cref="NotSupportedException">Thrown when the dialect does not support introspection.</exception>
        public async Task<TableSchema?> ReadTableAsync(string tableName, CancellationToken token = default)
        {
            RequireName(tableName);
            token.ThrowIfCancellationRequested();
            MigrationSession session = await MigrationSession.OpenAsync(_Executor, token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                return await ReadTableAsync(session, tableName, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reads several tables; tables that do not exist are omitted from the result.
        /// </summary>
        /// <param name="tableNames">Table names. Must not be null.</param>
        /// <returns>Existing tables keyed by name (case-insensitive).</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableNames is null.</exception>
        public Dictionary<string, TableSchema> ReadTables(IEnumerable<string> tableNames)
        {
            ArgumentNullException.ThrowIfNull(tableNames);
            using MigrationSession session = MigrationSession.Open(_Executor);
            return ReadTables(session, tableNames);
        }

        /// <summary>
        /// Reads several tables; tables that do not exist are omitted from the result.
        /// </summary>
        /// <param name="tableNames">Table names. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Existing tables keyed by name (case-insensitive).</returns>
        /// <exception cref="ArgumentNullException">Thrown when tableNames is null.</exception>
        public async Task<Dictionary<string, TableSchema>> ReadTablesAsync(IEnumerable<string> tableNames, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(tableNames);
            token.ThrowIfCancellationRequested();
            MigrationSession session = await MigrationSession.OpenAsync(_Executor, token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                return await ReadTablesAsync(session, tableNames, token).ConfigureAwait(false);
            }
        }

        #endregion

        #region Private-Methods

        internal static SqlCommandExecutor CreateExecutor(ISqlDialect dialect, IConnectionFactory connectionFactory, SqlRepositoryOptions? options, string tableName, Action<SqlStatement>? onExecuted)
        {
            return new SqlCommandExecutor(
                dialect, connectionFactory, options ?? new SqlRepositoryOptions(), typeof(DbConnection), typeof(DatabaseSchemaReader), tableName,
                onExecuted ?? (_ => { }));
        }

        internal static bool TableExists(MigrationSession session, string tableName)
        {
            return session.Query(session.Dialect.TableExistsQuery(tableName), "SCHEMA", r => 1).Count > 0;
        }

        internal static async Task<bool> TableExistsAsync(MigrationSession session, string tableName, CancellationToken token)
        {
            List<int> rows = await session.QueryAsync(session.Dialect.TableExistsQuery(tableName), "SCHEMA", r => 1, token).ConfigureAwait(false);
            return rows.Count > 0;
        }

        internal static TableSchema? ReadTable(MigrationSession session, string tableName)
        {
            if (!TableExists(session, tableName)) return null;
            List<ColumnSchema> columns = session.Query(session.Dialect.ColumnSchemaQuery(tableName), "SCHEMA", ReadColumn);
            List<IndexColumnRow> indexRows = session.Query(session.Dialect.IndexSchemaQuery(tableName), "SCHEMA", ReadIndexColumnRow);
            return Build(tableName, columns, indexRows);
        }

        internal static async Task<TableSchema?> ReadTableAsync(MigrationSession session, string tableName, CancellationToken token)
        {
            if (!await TableExistsAsync(session, tableName, token).ConfigureAwait(false)) return null;
            List<ColumnSchema> columns = await session.QueryAsync(session.Dialect.ColumnSchemaQuery(tableName), "SCHEMA", ReadColumn, token).ConfigureAwait(false);
            List<IndexColumnRow> indexRows = await session.QueryAsync(session.Dialect.IndexSchemaQuery(tableName), "SCHEMA", ReadIndexColumnRow, token).ConfigureAwait(false);
            return Build(tableName, columns, indexRows);
        }

        internal static Dictionary<string, TableSchema> ReadTables(MigrationSession session, IEnumerable<string> tableNames)
        {
            Dictionary<string, TableSchema> tables = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (string name in tableNames.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                TableSchema? table = ReadTable(session, name);
                if (table != null) tables[name] = table;
            }

            return tables;
        }

        internal static async Task<Dictionary<string, TableSchema>> ReadTablesAsync(MigrationSession session, IEnumerable<string> tableNames, CancellationToken token)
        {
            Dictionary<string, TableSchema> tables = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (string name in tableNames.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                TableSchema? table = await ReadTableAsync(session, name, token).ConfigureAwait(false);
                if (table != null) tables[name] = table;
            }

            return tables;
        }

        private static void RequireName(string tableName)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
        }

        private static ColumnSchema ReadColumn(DbDataReader reader)
        {
            return new ColumnSchema(
                ReadString(reader, 0),
                ReadString(reader, 1),
                ReadInt64(reader, 2) == 1,
                ReadLength(reader, 3),
                ReadInt64(reader, 4) == 1,
                0);
        }

        private static IndexColumnRow ReadIndexColumnRow(DbDataReader reader)
        {
            return new IndexColumnRow(
                ReadString(reader, 0),
                ReadString(reader, 1),
                ReadInt64(reader, 2) == 1,
                ReadInt64(reader, 3) ?? 0,
                ReadInt64(reader, 4) == 1);
        }

        private static TableSchema Build(string tableName, List<ColumnSchema> columns, List<IndexColumnRow> indexRows)
        {
            List<ColumnSchema> ordered = new List<ColumnSchema>(columns.Count);
            for (int i = 0; i < columns.Count; i++)
            {
                ColumnSchema c = columns[i];
                ordered.Add(new ColumnSchema(c.Name, c.DataType, c.IsNullable, c.MaxLength, c.IsPrimaryKey, i));
            }

            List<IndexSchema> indexes = new List<IndexSchema>();
            foreach (IGrouping<string, IndexColumnRow> group in indexRows.GroupBy(r => r.IndexName, StringComparer.Ordinal))
            {
                List<IndexColumnRow> rows = group.OrderBy(r => r.Position).ToList();
                indexes.Add(new IndexSchema(
                    group.Key,
                    rows.Where(r => !r.IsIncluded).Select(r => r.ColumnName),
                    rows[0].IsUnique,
                    rows.Where(r => r.IsIncluded).Select(r => r.ColumnName)));
            }

            return new TableSchema(tableName, ordered, indexes);
        }

        private static string ReadString(DbDataReader reader, int ordinal)
        {
            object value = reader.GetValue(ordinal);
            if (value == DBNull.Value) return string.Empty;
            if (value is byte[] bytes) return Encoding.UTF8.GetString(bytes);
            return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        }

        private static long? ReadInt64(DbDataReader reader, int ordinal)
        {
            object value = reader.GetValue(ordinal);
            if (value == DBNull.Value) return null;
            if (value is ulong unsigned) return unsigned > long.MaxValue ? long.MaxValue : (long)unsigned;
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        private static int? ReadLength(DbDataReader reader, int ordinal)
        {
            long? value = ReadInt64(reader, ordinal);
            if (value == null) return null;
            if (value.Value < 0 || value.Value > int.MaxValue) return -1;
            return (int)value.Value;
        }

        #endregion
    }
}
