namespace Durable.DuckDb
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Numerics;
    using System.Threading;
    using System.Threading.Tasks;
    using DuckDB.NET.Data;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// DuckDB repository for <typeparamref name="T"/>. All behavior comes from <see cref="SqlRepository{T}"/>; this class
    /// supplies the DuckDB dialect and connection handling, a bulk insert through the DuckDB Appender, and database file
    /// creation. See <see cref="DuckDbConnectionFactory"/> for in-memory database lifetime and concurrency semantics.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class DuckDbRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : SqlRepository<T> where T : class, new()
    {
        #region Constructors-and-Factories

        /// <summary>
        /// Creates a repository from a connection string. The repository owns its connection factory and disposes it, so a
        /// <c>:memory:</c> database is private to this repository; share one <see cref="DuckDbConnectionFactory"/> to use
        /// one in-memory database from several repositories.
        /// </summary>
        /// <param name="connectionString">DuckDB.NET connection string. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is invalid.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key.</exception>
        public DuckDbRepository(string connectionString, SqlRepositoryOptions? options = null)
            : this(DuckDbRepositorySettings.Parse(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), connectionString, options)
        {
        }

        /// <summary>
        /// Creates a repository from settings. The repository owns its connection factory and disposes it.
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the settings lack a data source.</exception>
        public DuckDbRepository(DuckDbRepositorySettings settings, SqlRepositoryOptions? options = null)
            : this(settings ?? throw new ArgumentNullException(nameof(settings)), settings.BuildConnectionString(), options)
        {
        }

        /// <summary>
        /// Creates a repository on a shared connection factory. The factory is not disposed with the repository.
        /// </summary>
        /// <param name="connectionFactory">Factory producing <see cref="DuckDBConnection"/> instances. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory is null.</exception>
        public DuckDbRepository(IConnectionFactory connectionFactory, SqlRepositoryOptions? options = null)
            : base(DuckDbDialect.Default, connectionFactory, false, typeof(DuckDBConnection), null, options)
        {
        }

        private DuckDbRepository(DuckDbRepositorySettings settings, string connectionString, SqlRepositoryOptions? options)
            : base(DuckDbDialect.Default, new DuckDbConnectionFactory(connectionString), true, typeof(DuckDBConnection), settings, options)
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Ensures the database exists: creates the directory of a database file and opens a connection, which creates the
        /// file. In-memory databases need no action.
        /// </summary>
        public override void CreateDatabaseIfNotExists()
        {
            ThrowIfDisposed();
            EnsureDirectory();
            using DbConnection connection = ConnectionFactory.OpenConnection();
        }

        /// <summary>
        /// Ensures the database exists: creates the directory of a database file and opens a connection.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        public override async Task CreateDatabaseIfNotExistsAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            EnsureDirectory();
            DbConnection connection = await ConnectionFactory.OpenConnectionAsync(token).ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Inserts rows with the DuckDB Appender when every value's type matches its column type exactly (as it does for
        /// tables created by Durable), generating auto-increment keys from their sequences; otherwise with a prepared INSERT
        /// executed once per row inside the transaction.
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <returns>Rows inserted.</returns>
        protected override long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            if (entities.Count == 0) return 0;
            AppenderPlan? plan = PlanAppender(lease, entities);
            if (plan != null) return Append(lease, plan, entities.Count);

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
        /// Inserts rows with the DuckDB Appender, or a prepared INSERT when types do not match (see <see cref="BulkInsertCore"/>).
        /// The Appender itself is synchronous (DuckDB runs in-process).
        /// </summary>
        /// <param name="lease">Lease inside a transaction.</param>
        /// <param name="entities">Prepared entities.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected override async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (entities.Count == 0) return 0;
            AppenderPlan? plan = PlanAppender(lease, entities);
            if (plan != null) return Append(lease, plan, entities.Count);

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

        private void EnsureDirectory()
        {
            if (Settings is DuckDbRepositorySettings settings && !settings.IsInMemory && !string.IsNullOrEmpty(settings.DataSource))
            {
                string? directory = Path.GetDirectoryName(Path.GetFullPath(settings.DataSource));
                if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            }
        }

        private AppenderPlan? PlanAppender(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            if (lease.Connection is not DuckDBConnection) return null;

            string tableName = Metadata.TableName;
            int dot = tableName.LastIndexOf('.');
            string? schema = dot > 0 ? tableName.Substring(0, dot) : null;
            string table = dot >= 0 ? tableName.Substring(dot + 1) : tableName;

            List<SqlParameterValue> parameters = new List<SqlParameterValue> { new SqlParameterValue(Dialect.FormatParameterName(0), table) };
            string schemaSql = "current_schema()";
            if (schema != null)
            {
                parameters.Add(new SqlParameterValue(Dialect.FormatParameterName(1), schema));
                schemaSql = Dialect.FormatParameterName(1);
            }

            SqlStatement columnsQuery = new SqlStatement(
                "SELECT column_name, data_type, column_default, schema_name FROM duckdb_columns() WHERE database_name = current_database() AND schema_name = "
                + schemaSql + " AND table_name = " + Dialect.FormatParameterName(0) + " ORDER BY column_index",
                parameters);

            List<string> names = new List<string>();
            List<string> types = new List<string>();
            List<string?> defaults = new List<string?>();
            string? resolvedSchema = null;
            using (DbCommand command = CreateCommand(lease, columnsQuery))
            using (DbDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    names.Add(reader.GetString(0));
                    types.Add(reader.GetString(1));
                    defaults.Add(reader.IsDBNull(2) ? null : reader.GetString(2));
                    resolvedSchema = reader.GetString(3);
                }
            }

            if (names.Count == 0 || resolvedSchema == null) return null;

            Dictionary<string, int> insertIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < InsertColumns.Count; i++) insertIndex[InsertColumns[i].Name] = i;

            int[] sources = new int[names.Count];
            string?[] sequenceDefaults = new string?[names.Count];
            int matched = 0;
            for (int c = 0; c < names.Count; c++)
            {
                if (insertIndex.TryGetValue(names[c], out int index))
                {
                    sources[c] = index;
                    matched++;
                }
                else if (defaults[c] == null)
                {
                    sources[c] = -1;
                }
                else if (defaults[c]!.StartsWith("nextval(", StringComparison.OrdinalIgnoreCase))
                {
                    sources[c] = -2;
                    sequenceDefaults[c] = defaults[c];
                }
                else
                {
                    return null;
                }
            }

            if (matched != InsertColumns.Count) return null;

            object[][] rows = new object[entities.Count][];
            for (int r = 0; r < entities.Count; r++)
            {
                object[] row = new object[InsertColumns.Count];
                for (int i = 0; i < InsertColumns.Count; i++)
                {
                    object value = DatabaseValue(entities[r], InsertColumns[i]);
                    row[i] = value;
                }

                rows[r] = row;
            }

            for (int c = 0; c < names.Count; c++)
            {
                if (sources[c] < 0) continue;
                foreach (object[] row in rows)
                {
                    if (!Appendable(row[sources[c]], types[c])) return null;
                }
            }

            object?[][] generated = new object?[names.Count][];
            for (int c = 0; c < names.Count; c++)
            {
                if (sources[c] != -2) continue;
                generated[c] = NextValues(lease, sequenceDefaults[c]!, types[c], entities.Count);
            }

            return new AppenderPlan(resolvedSchema, table, sources, rows, generated);
        }

        private object?[] NextValues(ConnectionLease lease, string defaultExpression, string columnType, int count)
        {
            // Cast to the column type: the Appender writes the value's own width, so a BIGINT from nextval must become INTEGER.
            object?[] values = new object?[count];
            SqlStatement statement = new SqlStatement("SELECT CAST(" + defaultExpression + " AS " + columnType + ") FROM range(" + count.ToString(System.Globalization.CultureInfo.InvariantCulture) + ")");
            using DbCommand command = CreateCommand(lease, statement);
            using DbDataReader reader = command.ExecuteReader();
            int i = 0;
            while (reader.Read() && i < count) values[i++] = reader.GetValue(0);
            return values;
        }

        private static long Append(ConnectionLease lease, AppenderPlan plan, int count)
        {
            DuckDBConnection connection = (DuckDBConnection)lease.Connection;
            using (DuckDBAppender appender = connection.CreateAppender(plan.Schema, plan.Table))
            {
                for (int r = 0; r < count; r++)
                {
                    IDuckDBAppenderRow row = appender.CreateRow();
                    for (int c = 0; c < plan.Sources.Length; c++)
                    {
                        int source = plan.Sources[c];
                        object? value = source >= 0 ? plan.Rows[r][source] : source == -2 ? plan.Generated[c]![r] : null;
                        row = AppendValue(row, value);
                    }

                    row.EndRow();
                }
            }

            return count;
        }

        private static IDuckDBAppenderRow AppendValue(IDuckDBAppenderRow row, object? value)
        {
            switch (value)
            {
                case null: return row.AppendNullValue();
                case DBNull: return row.AppendNullValue();
                case bool v: return row.AppendValue((bool?)v);
                case sbyte v: return row.AppendValue((sbyte?)v);
                case byte v: return row.AppendValue((byte?)v);
                case short v: return row.AppendValue((short?)v);
                case ushort v: return row.AppendValue((ushort?)v);
                case int v: return row.AppendValue((int?)v);
                case uint v: return row.AppendValue((uint?)v);
                case long v: return row.AppendValue((long?)v);
                case ulong v: return row.AppendValue((ulong?)v);
                case BigInteger v: return row.AppendValue((BigInteger?)v);
                case float v: return row.AppendValue((float?)v);
                case double v: return row.AppendValue((double?)v);
                case decimal v: return row.AppendValue((decimal?)v);
                case string v: return row.AppendValue(v);
                case Guid v: return row.AppendValue((Guid?)v);
                case DateTime v: return row.AppendValue((DateTime?)v);
                case DateTimeOffset v: return row.AppendValue((DateTimeOffset?)v);
                case DateOnly v: return row.AppendValue((DateOnly?)v);
                case TimeOnly v: return row.AppendValue((TimeOnly?)v);
                case TimeSpan v: return row.AppendValue((TimeSpan?)v);
                case byte[] v: return row.AppendValue(v);
                default: return row.AppendValue(value.ToString());
            }
        }

        private static bool Appendable(object value, string columnType)
        {
            if (value == DBNull.Value) return true;
            string type = columnType.ToUpperInvariant();
            switch (value)
            {
                case bool: return type == "BOOLEAN";
                case sbyte: return type == "TINYINT";
                case byte: return type == "UTINYINT";
                case short: return type == "SMALLINT";
                case ushort: return type == "USMALLINT";
                case int: return type == "INTEGER";
                case uint: return type == "UINTEGER";
                case long: return type == "BIGINT";
                case ulong: return type == "UBIGINT";
                case BigInteger: return type == "HUGEINT";
                case float: return type == "FLOAT";
                case double: return type == "DOUBLE";
                case decimal: return type.StartsWith("DECIMAL", StringComparison.Ordinal);
                case string: return type == "VARCHAR" || type == "JSON";
                case Guid: return type == "UUID";
                case DateTime: return type == "TIMESTAMP";
                case DateTimeOffset: return type == "TIMESTAMP WITH TIME ZONE";
                case DateOnly: return type == "DATE";
                case TimeOnly: return type == "TIME";
                case TimeSpan: return type == "INTERVAL";
                case byte[]: return type == "BLOB";
                default: return false;
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
            return CreateCommand(lease, builder.Build());
        }

        private void BindRow(DbCommand command, T entity)
        {
            for (int i = 0; i < InsertColumns.Count; i++) command.Parameters[i].Value = DatabaseValue(entity, InsertColumns[i]);
        }

        #endregion
    }
}
