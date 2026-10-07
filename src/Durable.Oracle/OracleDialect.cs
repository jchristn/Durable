namespace Durable.Oracle
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;
    using Durable;
    using Durable.Query;
    using Durable.Sql;
    using global::Oracle.ManagedDataAccess.Client;

    /// <summary>
    /// Oracle Database dialect (Oracle 19c and later; tested on Oracle Database 23ai Free). Identifiers are quoted and, by
    /// default, folded to upper case so they match unquoted names in hand-written SQL and in existing schemas; parameters
    /// are bound by name (<c>:p0</c>); paging uses OFFSET/FETCH; identity keys come back through
    /// <c>RETURNING ... INTO</c> output parameters; upsert uses MERGE; several statements in one command run as one PL/SQL
    /// block; booleans are NUMBER(1); GUIDs are RAW(16); strings are VARCHAR2 with character length semantics.
    /// Oracle stores an empty string as NULL (<see cref="TreatsEmptyStringAsNull"/>), commits DDL implicitly
    /// (<see cref="SupportsTransactionalDdl"/> is false) and limits IN lists to 1000 items (<see cref="MaxInListItems"/>).
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class OracleDialect : SqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance (identifiers folded to upper case, unbounded strings as VARCHAR2(4000 CHAR)).
        /// </summary>
        public static OracleDialect Default { get; } = new OracleDialect();

        /// <inheritdoc />
        public override RepositoryType RepositoryType => RepositoryType.Oracle;

        /// <inheritdoc />
        public override string DbSystemName => "oracle";

        /// <inheritdoc />
        public override int MaxParameters => 2000;

        /// <summary>
        /// Gets 1000: Oracle rejects IN lists with more items (ORA-01795), so longer lists are split.
        /// </summary>
        public override int MaxInListItems => 1000;

        /// <inheritdoc />
        public override string RecursiveCteKeyword => "WITH";

        /// <summary>
        /// Gets <see cref="InsertKeyStrategy.ReturningInto"/>: generated keys are read from output parameters.
        /// </summary>
        public override InsertKeyStrategy InsertKeyStrategy => InsertKeyStrategy.ReturningInto;

        /// <summary>
        /// Gets "VALUES (DEFAULT)": Oracle has no DEFAULT VALUES clause; an entity whose only column is its identity key
        /// inserts DEFAULT into that column.
        /// </summary>
        public override string InsertDefaultValuesClause => "VALUES (DEFAULT)";

        /// <inheritdoc />
        public override string StringCastType => "VARCHAR2(4000)";

        /// <summary>
        /// Gets "BEGIN ": statements sent together run as one anonymous PL/SQL block.
        /// </summary>
        public override string StatementBatchPrefix => "BEGIN ";

        /// <summary>
        /// Gets "; END;", closing the PL/SQL block opened by <see cref="StatementBatchPrefix"/>.
        /// </summary>
        public override string StatementBatchSuffix => "; END;";

        /// <summary>
        /// Gets false: Oracle 19c has no multi-row VALUES list, so rows are inserted by one statement each inside a PL/SQL block.
        /// </summary>
        public override bool SupportsMultiRowInsert => false;

        /// <summary>
        /// Gets " FROM DUAL".
        /// </summary>
        public override string SingleRowFromClause => " FROM DUAL";

        /// <summary>
        /// Gets true: Oracle treats a zero-length VARCHAR2 as NULL, so an empty string reads back as NULL and comparing a
        /// column with an empty string matches no row.
        /// </summary>
        public override bool TreatsEmptyStringAsNull => true;

        /// <summary>
        /// Gets whether identifiers are folded to upper case before quoting. Default: true. When true, <c>[Entity("people")]</c>
        /// maps to the table PEOPLE, which unquoted SQL (<c>SELECT * FROM people</c>) also resolves to; when false, names
        /// keep their case and hand-written SQL must quote them.
        /// </summary>
        public bool UpperCaseIdentifiers { get; }

        /// <summary>
        /// Gets the column type used for strings without a maximum length. Default: "VARCHAR2(4000 CHAR)" (comparable,
        /// sortable and groupable; Oracle limits it to 4000 bytes unless MAX_STRING_SIZE is EXTENDED). Strings with a
        /// maximum length above 4000 and JSON columns are always CLOB.
        /// </summary>
        public string UnboundedStringType { get; }

        // Migrations

        /// <summary>
        /// Gets false: Oracle commits implicitly before and after every DDL statement.
        /// </summary>
        public override bool SupportsTransactionalDdl => false;

        /// <summary>
        /// Gets 128: the identifier limit of Oracle 12.2 and later.
        /// </summary>
        public override int MaxIdentifierLength => 128;

        /// <inheritdoc />
        public override string CurrentUtcTimestampSql => "SYS_EXTRACT_UTC(SYSTIMESTAMP)";

        /// <summary>
        /// Gets "SET TRANSACTION READ WRITE": Oracle starts transactions implicitly.
        /// </summary>
        public override string ScriptBeginTransactionSql => "SET TRANSACTION READ WRITE";

        #endregion

        #region Private-Members

        private static readonly Regex _PlSqlBlock = new Regex("^\\s*(BEGIN|DECLARE)\\b", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="OracleDataTypeConverter"/>.</param>
        /// <param name="upperCaseIdentifiers">Whether identifiers are folded to upper case before quoting. Default: true.</param>
        /// <param name="unboundedStringType">Column type for strings without a maximum length. Default: "VARCHAR2(4000 CHAR)";
        /// use "CLOB" for unlimited text (CLOB columns cannot be compared with =, sorted, grouped or indexed).</param>
        /// <exception cref="ArgumentException">Thrown when unboundedStringType is null or whitespace.</exception>
        public OracleDialect(IDataTypeConverter? converter = null, bool upperCaseIdentifiers = true, string unboundedStringType = "VARCHAR2(4000 CHAR)")
            : base(converter ?? new OracleDataTypeConverter())
        {
            if (string.IsNullOrWhiteSpace(unboundedStringType)) throw new ArgumentException("The unbounded string type cannot be empty.", nameof(unboundedStringType));
            UpperCaseIdentifiers = upperCaseIdentifiers;
            UnboundedStringType = unboundedStringType;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Quotes an identifier, folding it to upper case first when <see cref="UpperCaseIdentifiers"/> is true.
        /// Dotted names (owner.table) are quoted per part.
        /// </summary>
        /// <param name="identifier">Identifier. Must not be null or empty.</param>
        /// <returns>The quoted identifier.</returns>
        /// <exception cref="ArgumentException">Thrown when identifier is null or empty.</exception>
        public override string QuoteIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier)) throw new ArgumentException("Identifier cannot be null or empty.", nameof(identifier));
            return base.QuoteIdentifier(FoldIdentifier(identifier));
        }

        /// <summary>
        /// Returns an identifier as Oracle stores it in the data dictionary: upper case when
        /// <see cref="UpperCaseIdentifiers"/> is true, unchanged otherwise.
        /// </summary>
        /// <param name="identifier">Identifier. Must not be null.</param>
        /// <returns>The stored form.</returns>
        /// <exception cref="ArgumentNullException">Thrown when identifier is null.</exception>
        public string FoldIdentifier(string identifier)
        {
            ArgumentNullException.ThrowIfNull(identifier);
            return UpperCaseIdentifiers ? identifier.ToUpperInvariant() : identifier;
        }

        /// <inheritdoc />
        public override string FormatParameterName(int index)
        {
            return ":p" + index.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Binds parameters by name (ODP.NET binds by position by default).
        /// </summary>
        /// <param name="command">The provider command. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when command is null.</exception>
        public override void ConfigureCommand(DbCommand command)
        {
            ArgumentNullException.ThrowIfNull(command);
            if (command is OracleCommand oracleCommand) oracleCommand.BindByName = true;
        }

        /// <summary>
        /// Types parameters for Oracle: <see cref="DateTime"/> as TIMESTAMP (the driver default, DATE, drops fractional
        /// seconds), <see cref="DateTimeOffset"/> as TIMESTAMP WITH TIME ZONE, <see cref="TimeSpan"/> as INTERVAL DAY TO
        /// SECOND, long text and JSON as CLOB, long binary as BLOB, and output parameters as .NET values.
        /// </summary>
        /// <param name="parameter">The provider parameter. Must not be null.</param>
        /// <param name="value">The engine parameter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public override void ConfigureParameter(DbParameter parameter, SqlParameterValue value)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(value);
            if (parameter is not OracleParameter oracle) return;

            if (value.Direction != ParameterDirection.Input)
            {
                switch (value.DbType)
                {
                    case DbType.Int16: oracle.OracleDbTypeEx = OracleDbType.Int16; break;
                    case DbType.Int32: oracle.OracleDbTypeEx = OracleDbType.Int32; break;
                    case DbType.Int64: oracle.OracleDbTypeEx = OracleDbType.Int64; break;
                    case DbType.Decimal: oracle.OracleDbTypeEx = OracleDbType.Decimal; break;
                    case DbType.String:
                        oracle.OracleDbTypeEx = OracleDbType.Varchar2;
                        if (oracle.Size == 0) oracle.Size = 4000;
                        break;
                }

                return;
            }

            string? columnType = value.Column != null ? GetColumnType(value.Column) : null;
            switch (value.Value)
            {
                case DateTime:
                    oracle.OracleDbType = OracleDbType.TimeStamp;
                    break;
                case DateTimeOffset:
                    oracle.OracleDbType = OracleDbType.TimeStampTZ;
                    break;
                case TimeSpan:
                    oracle.OracleDbType = OracleDbType.IntervalDS;
                    break;
                case string text:
                    if (text.Length > 2000 || IsLob(columnType, "CLOB")) oracle.OracleDbType = OracleDbType.Clob;
                    break;
                case byte[] bytes:
                    oracle.OracleDbType = bytes.Length > 2000 || IsLob(columnType, "BLOB") ? OracleDbType.Blob : OracleDbType.Raw;
                    break;
                case null:
                case DBNull:
                    if (IsLob(columnType, "CLOB")) oracle.OracleDbType = OracleDbType.Clob;
                    else if (IsLob(columnType, "BLOB")) oracle.OracleDbType = OracleDbType.Blob;
                    break;
            }
        }

        /// <inheritdoc />
        public override string BooleanLiteral(bool value)
        {
            return value ? "1" : "0";
        }

        /// <summary>
        /// Divides two expressions; integer operands truncate toward zero like C# (<c>TRUNC(a / b)</c>).
        /// </summary>
        /// <param name="left">Dividend SQL.</param>
        /// <param name="right">Divisor SQL.</param>
        /// <param name="integerOperands">Whether both operands are integer-typed.</param>
        /// <returns>The division expression.</returns>
        public override string Divide(string left, string right, bool integerOperands)
        {
            return integerOperands ? "TRUNC(" + left + " / " + right + ")" : "(" + left + " / " + right + ")";
        }

        /// <summary>
        /// Returns <c>MOD(left, right)</c>; like C#, the sign follows the dividend.
        /// </summary>
        /// <param name="left">Dividend SQL.</param>
        /// <param name="right">Divisor SQL.</param>
        /// <returns>The remainder expression.</returns>
        public override string Modulo(string left, string right)
        {
            return "MOD(" + left + ", " + right + ")";
        }

        /// <summary>
        /// Returns <c>expression IS NULL</c>: Oracle stores an empty string as NULL, so no stored string has zero length.
        /// </summary>
        /// <param name="expression">String SQL expression.</param>
        /// <returns>The condition.</returns>
        public override string IsEmptyString(string expression)
        {
            return "(" + expression + " IS NULL)";
        }

        /// <summary>
        /// Returns " ASC NULLS FIRST" or " DESC NULLS LAST", LINQ's null ordering (Oracle sorts NULLs last when ascending by default).
        /// </summary>
        /// <param name="descending">Whether the key sorts descending.</param>
        /// <returns>The suffix.</returns>
        public override string OrderDirection(bool descending)
        {
            return descending ? " DESC NULLS LAST" : " ASC NULLS FIRST";
        }

        /// <inheritdoc />
        public override string SetOperationKeyword(SetOperationType operation)
        {
            return operation == SetOperationType.Except ? "MINUS" : base.SetOperationKeyword(operation);
        }

        /// <inheritdoc />
        public override string TranslateFunction(QueryFunction function, IReadOnlyList<string> arguments)
        {
            string a0 = arguments.Count > 0 ? arguments[0] : string.Empty;
            string a1 = arguments.Count > 1 ? arguments[1] : string.Empty;
            switch (function)
            {
                case QueryFunction.Ceiling: return "CEIL(" + a0 + ")";
                case QueryFunction.DayOfYear: return "TO_NUMBER(TO_CHAR(" + a0 + ", 'DDD'))";
                case QueryFunction.DayOfWeek: return "MOD(TRUNC(" + a0 + ") - TRUNC(" + a0 + ", 'IW') + 1, 7)";
                case QueryFunction.Date: return "CAST(TRUNC(" + a0 + ") AS TIMESTAMP(7))";
                case QueryFunction.AddYears: return AddMonths(a0, "12 * (" + a1 + ")");
                case QueryFunction.AddMonths: return AddMonths(a0, a1);
                case QueryFunction.AddDays: return "(" + a0 + " + NUMTODSINTERVAL(" + a1 + ", 'DAY'))";
                case QueryFunction.AddHours: return "(" + a0 + " + NUMTODSINTERVAL(" + a1 + ", 'HOUR'))";
                case QueryFunction.AddMinutes: return "(" + a0 + " + NUMTODSINTERVAL(" + a1 + ", 'MINUTE'))";
                case QueryFunction.AddSeconds: return "(" + a0 + " + NUMTODSINTERVAL(" + a1 + ", 'SECOND'))";
                default:
                    return base.TranslateFunction(function, arguments);
            }
        }

        /// <summary>
        /// Appends <c>OFFSET n ROWS</c> and <c>FETCH NEXT m ROWS ONLY</c> (Oracle 12c and later).
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="skip">Rows to skip; null for none.</param>
        /// <param name="take">Rows to take; null for unlimited.</param>
        /// <param name="hasOrderBy">Whether an ORDER BY was emitted.</param>
        /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
        public override void AppendPaging(SqlStatementBuilder builder, int? skip, int? take, bool hasOrderBy)
        {
            ArgumentNullException.ThrowIfNull(builder);
            if (skip.HasValue && skip.Value > 0) builder.Append(" OFFSET ").Append(skip.Value.ToString(CultureInfo.InvariantCulture)).Append(" ROWS");
            if (take.HasValue) builder.Append(" FETCH NEXT ").Append(take.Value.ToString(CultureInfo.InvariantCulture)).Append(" ROWS ONLY");
        }

        /// <summary>
        /// Appends a MERGE statement keyed on the conflict columns.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="insertColumns">Columns being inserted. Must not be null.</param>
        /// <param name="placeholders">Placeholders aligned with insertColumns. Must not be null.</param>
        /// <param name="conflictColumns">Key columns identifying the row. Must not be null or empty.</param>
        /// <param name="updateColumns">Columns updated on conflict; may be empty.</param>
        /// <param name="versionPlaceholder">Placeholder for the incremented version; null when there is no version column.</param>
        /// <exception cref="ArgumentNullException">Thrown when builder or metadata is null.</exception>
        public override void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns, string? versionPlaceholder = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            builder.Append("MERGE INTO ").AppendIdentifier(metadata.TableName).Append(" durable_target USING (SELECT ");
            for (int i = 0; i < insertColumns.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(placeholders[i]).Append(" AS ").Append(QuoteIdentifier(insertColumns[i].Name));
            }

            builder.Append(" FROM DUAL) durable_source ON (")
                .Append(string.Join(" AND ", conflictColumns.Select(c => "durable_target." + QuoteIdentifier(c.Name) + " = durable_source." + QuoteIdentifier(c.Name))))
                .Append(")");
            if (updateColumns.Count > 0)
            {
                builder.Append(" WHEN MATCHED THEN UPDATE SET ")
                    .Append(string.Join(", ", updateColumns.Select(c => "durable_target." + QuoteIdentifier(c.Name) + " = " + (c.IsVersion && versionPlaceholder != null ? versionPlaceholder : "durable_source." + QuoteIdentifier(c.Name)))));
            }

            builder.Append(" WHEN NOT MATCHED THEN INSERT (")
                .Append(string.Join(", ", insertColumns.Select(c => QuoteIdentifier(c.Name))))
                .Append(") VALUES (")
                .Append(string.Join(", ", insertColumns.Select(c => "durable_source." + QuoteIdentifier(c.Name))))
                .Append(")");
        }

        /// <summary>
        /// Returns null: Oracle has no RELEASE SAVEPOINT; savepoints end with the transaction.
        /// </summary>
        /// <param name="name">Savepoint name.</param>
        /// <returns>Null.</returns>
        public override string? ReleaseSavepointSql(string name)
        {
            return null;
        }

        /// <summary>
        /// Returns the Oracle column type: NUMBER(p) for integers, NUMBER(1) for booleans, NUMBER(38,10) for decimals,
        /// BINARY_FLOAT / BINARY_DOUBLE, TIMESTAMP(7), TIMESTAMP(7) WITH TIME ZONE, DATE for <see cref="DateOnly"/>,
        /// INTERVAL DAY TO SECOND for <see cref="TimeSpan"/> and <see cref="TimeOnly"/>, RAW(16) for GUIDs, VARCHAR2(n CHAR)
        /// for strings (<see cref="UnboundedStringType"/> without a maximum length, CLOB above 4000), CLOB for JSON, and
        /// RAW or BLOB for binary data (RAW for keys, indexes, version counters and lengths up to 2000).
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The SQL type.</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null.</exception>
        public override string GetColumnType(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            Type type = column.Converter?.ProviderType ?? column.ClrType;
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (column.IsJson) return "CLOB";
            if (type.IsEnum) return column.EnumAsString ? BoundedString(column.MaxLength > 0 ? column.MaxLength : 64) : "NUMBER(10)";
            if (type == typeof(bool)) return "NUMBER(1)";
            if (type == typeof(byte)) return "NUMBER(3)";
            if (type == typeof(sbyte) || type == typeof(short)) return "NUMBER(5)";
            if (type == typeof(ushort) || type == typeof(int)) return "NUMBER(10)";
            if (type == typeof(uint) || type == typeof(long)) return "NUMBER(19)";
            if (type == typeof(ulong)) return "NUMBER(20)";
            if (type == typeof(float)) return "BINARY_FLOAT";
            if (type == typeof(double)) return "BINARY_DOUBLE";
            if (type == typeof(decimal)) return "NUMBER(38,10)";
            if (type == typeof(DateTime)) return "TIMESTAMP(7)";
            if (type == typeof(DateTimeOffset)) return "TIMESTAMP(7) WITH TIME ZONE";
            if (type == typeof(DateOnly)) return "DATE";
            if (type == typeof(TimeOnly)) return "INTERVAL DAY(0) TO SECOND(7)";
            if (type == typeof(TimeSpan)) return "INTERVAL DAY(9) TO SECOND(7)";
            if (type == typeof(Guid)) return "RAW(16)";
            if (type == typeof(byte[]))
            {
                if (column.MaxLength > 0 && column.MaxLength <= 2000) return "RAW(" + column.MaxLength.ToString(CultureInfo.InvariantCulture) + ")";
                if (column.IsVersion) return "RAW(16)";
                if (column.IsPrimaryKey || column.Indexes.Count > 0 || column.ForeignKey != null) return "RAW(2000)";
                return "BLOB";
            }

            if (type == typeof(char)) return "VARCHAR2(1 CHAR)";
            if (column.MaxLength > 4000) return "CLOB";
            if (column.MaxLength > 0) return BoundedString(column.MaxLength);
            if (column.IsPrimaryKey || column.Indexes.Count > 0 || column.ForeignKey != null) return BoundedString(450);
            return UnboundedStringType;
        }

        /// <summary>
        /// Appends CREATE TABLE inside a PL/SQL block that ignores ORA-00955 (name already used), so it is a no-op when
        /// the table exists (Oracle 19c has no CREATE TABLE IF NOT EXISTS).
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public override void AppendCreateTable(SqlStatementBuilder builder, EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            SqlStatementBuilder ddl = new SqlStatementBuilder(this);
            ddl.Append("CREATE TABLE ").AppendIdentifier(metadata.TableName).Append(" (");
            AppendTableBody(ddl, metadata);
            ddl.Append(")");
            builder.Append(IgnoreErrorBlock(ddl.ToString(), -955));
        }

        /// <summary>
        /// Returns true for nullable columns and for non-key string columns (VARCHAR2 and CLOB, including JSON and
        /// string-converted columns): Oracle stores an empty string as NULL, so a NOT NULL string column would reject
        /// empty strings that a non-nullable string property may hold.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>True when the column is declared NULL.</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null.</exception>
        public override bool ColumnAllowsNull(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            if (column.IsNullable) return true;
            if (column.IsPrimaryKey) return false;
            Type type = column.Converter?.ProviderType ?? column.ClrType;
            type = Nullable.GetUnderlyingType(type) ?? type;
            return column.IsJson || type == typeof(string) || type == typeof(char);
        }

        /// <inheritdoc />
        public override SqlStatement TableExistsQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            List<SqlParameterValue> parameters = new List<SqlParameterValue>();
            return new SqlStatement("SELECT 1 FROM ALL_TABLES t WHERE " + DictionaryFilter(tableName, "t", parameters), parameters);
        }

        /// <inheritdoc />
        public override SqlStatement ColumnNamesQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            List<SqlParameterValue> parameters = new List<SqlParameterValue>();
            return new SqlStatement("SELECT " + DictionaryName("c.COLUMN_NAME") + " FROM ALL_TAB_COLUMNS c WHERE " + DictionaryFilter(tableName, "c", parameters) + " ORDER BY c.COLUMN_ID", parameters);
        }

        /// <inheritdoc />
        public override SqlStatement IndexNamesQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            List<SqlParameterValue> parameters = new List<SqlParameterValue>();
            return new SqlStatement(
                "SELECT " + DictionaryName("i.INDEX_NAME") + " FROM ALL_INDEXES i WHERE " + DictionaryFilter(tableName, "i", parameters, "TABLE_OWNER") + " AND i.INDEX_TYPE <> 'LOB' " +
                "AND NOT EXISTS (SELECT 1 FROM ALL_CONSTRAINTS k WHERE k.OWNER = i.TABLE_OWNER AND k.TABLE_NAME = i.TABLE_NAME AND k.INDEX_NAME = i.INDEX_NAME AND k.CONSTRAINT_TYPE = 'P')",
                parameters);
        }

        /// <summary>
        /// Returns CREATE INDEX. Oracle has no covering (INCLUDE) columns, so includedColumns are ignored.
        /// </summary>
        /// <param name="indexName">Index name. Must not be null.</param>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <param name="columns">Column names in order. Must not be empty.</param>
        /// <param name="unique">Whether the index is unique.</param>
        /// <param name="includedColumns">Ignored.</param>
        /// <returns>SQL text.</returns>
        public override string CreateIndexSql(string indexName, string tableName, IReadOnlyList<string> columns, bool unique, IReadOnlyList<string>? includedColumns)
        {
            return base.CreateIndexSql(indexName, tableName, columns, unique, null);
        }

        // Migrations

        /// <inheritdoc />
        public override SqlStatement ColumnSchemaQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            List<SqlParameterValue> parameters = new List<SqlParameterValue>();
            return new SqlStatement(
                "SELECT " + DictionaryName("c.COLUMN_NAME") + ", " +
                "CASE WHEN c.DATA_TYPE IN ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR') THEN c.DATA_TYPE || '(' || c.CHAR_LENGTH || " +
                "CASE WHEN c.DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN CASE WHEN c.CHAR_USED = 'C' THEN ' CHAR' ELSE ' BYTE' END END || ')' " +
                "WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NULL AND c.DATA_SCALE IS NULL THEN 'NUMBER' " +
                "WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NULL THEN 'NUMBER(38)' " +
                "WHEN c.DATA_TYPE = 'NUMBER' AND NVL(c.DATA_SCALE, 0) = 0 THEN 'NUMBER(' || c.DATA_PRECISION || ')' " +
                "WHEN c.DATA_TYPE = 'NUMBER' THEN 'NUMBER(' || c.DATA_PRECISION || ',' || c.DATA_SCALE || ')' " +
                "WHEN c.DATA_TYPE = 'RAW' THEN 'RAW(' || c.DATA_LENGTH || ')' " +
                "ELSE c.DATA_TYPE END, " +
                "CASE WHEN c.NULLABLE = 'Y' THEN 1 ELSE 0 END, " +
                "CASE WHEN c.DATA_TYPE IN ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR') THEN c.CHAR_LENGTH WHEN c.DATA_TYPE IN ('CLOB', 'NCLOB') THEN -1 ELSE NULL END, " +
                "CASE WHEN EXISTS (SELECT 1 FROM ALL_CONSTRAINTS k JOIN ALL_CONS_COLUMNS kc ON kc.OWNER = k.OWNER AND kc.CONSTRAINT_NAME = k.CONSTRAINT_NAME " +
                "WHERE k.OWNER = c.OWNER AND k.TABLE_NAME = c.TABLE_NAME AND k.CONSTRAINT_TYPE = 'P' AND kc.COLUMN_NAME = c.COLUMN_NAME) THEN 1 ELSE 0 END, " +
                "CASE WHEN c.IDENTITY_COLUMN = 'YES' THEN 1 ELSE 0 END " +
                "FROM ALL_TAB_COLUMNS c WHERE " + DictionaryFilter(tableName, "c", parameters) + " ORDER BY c.COLUMN_ID",
                parameters);
        }

        /// <inheritdoc />
        public override SqlStatement TableNamesQuery()
        {
            return new SqlStatement(
                "SELECT " + DictionaryName("TABLE_NAME") + " FROM USER_TABLES WHERE DROPPED = 'NO' AND IOT_TYPE IS NULL AND SECONDARY = 'N' AND NESTED = 'NO' " +
                "AND TEMPORARY = 'N' ORDER BY 1");
        }

        /// <inheritdoc />
        public override SqlStatement IndexSchemaQuery(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            List<SqlParameterValue> parameters = new List<SqlParameterValue>();
            return new SqlStatement(
                "SELECT " + DictionaryName("i.INDEX_NAME") + ", " + DictionaryName("ic.COLUMN_NAME") + ", CASE WHEN i.UNIQUENESS = 'UNIQUE' THEN 1 ELSE 0 END, ic.COLUMN_POSITION, 0 " +
                "FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS ic ON ic.INDEX_OWNER = i.OWNER AND ic.INDEX_NAME = i.INDEX_NAME " +
                "WHERE " + DictionaryFilter(tableName, "i", parameters, "TABLE_OWNER") + " AND i.INDEX_TYPE <> 'LOB' " +
                "AND NOT EXISTS (SELECT 1 FROM ALL_CONSTRAINTS k WHERE k.OWNER = i.TABLE_OWNER AND k.TABLE_NAME = i.TABLE_NAME " +
                "AND k.INDEX_NAME = i.INDEX_NAME AND k.CONSTRAINT_TYPE IN ('P', 'U')) " +
                "ORDER BY 1, ic.COLUMN_POSITION",
                parameters);
        }

        /// <inheritdoc />
        public override string NormalizeColumnType(string columnType)
        {
            string type = base.NormalizeColumnType(columnType);
            type = Regex.Replace(type, "^(varchar2|char)\\((\\d+)\\)$", "$1($2 byte)");
            int paren = type.IndexOf('(');
            string name = paren < 0 ? type : type.Substring(0, paren);
            string suffix = paren < 0 ? string.Empty : type.Substring(paren);
            switch (name)
            {
                case "varchar": name = "varchar2"; break;
                case "integer":
                case "int":
                case "smallint":
                    return "number(38)";
                case "decimal":
                case "numeric": name = "number"; break;
                case "double precision": return "float(126)";
                case "real": return "float(63)";
            }

            return name + suffix;
        }

        /// <inheritdoc />
        public override string FormatLiteral(object? value)
        {
            switch (value)
            {
                case DateTime dateTime:
                    return "TIMESTAMP '" + dateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture) + "'";
                case DateTimeOffset offset:
                    return "TIMESTAMP '" + offset.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture) + "'";
                case DateOnly date:
                    return "DATE '" + date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'";
                case TimeOnly time:
                    return IntervalLiteral(time.ToTimeSpan());
                case TimeSpan span:
                    return IntervalLiteral(span);
                case Guid guid:
                    return BinaryLiteral(guid.ToByteArray(true));
                default:
                    return base.FormatLiteral(value);
            }
        }

        /// <inheritdoc />
        public override string CreateMigrationHistoryTableSql(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            string ddl = "CREATE TABLE " + QuoteIdentifier(tableName) + " ("
                + QuoteIdentifier("id") + " VARCHAR2(150 CHAR) NOT NULL PRIMARY KEY, "
                + QuoteIdentifier("description") + " VARCHAR2(1000 CHAR) NULL, "
                + QuoteIdentifier("applied_utc") + " TIMESTAMP(7) NOT NULL, "
                + QuoteIdentifier("duration_ms") + " NUMBER(19) NOT NULL)";
            return IgnoreErrorBlock(ddl, -955);
        }

        /// <summary>
        /// Returns a PL/SQL block that requests an exclusive DBMS_LOCK user lock (session-scoped, kept across commits) whose
        /// id is a hash of the lock name, and returns 1 or 0 as an implicit result set. Requires
        /// <c>GRANT EXECUTE ON SYS.DBMS_LOCK</c> to the connecting user; without it the block raises ORA-20901 naming the grant.
        /// </summary>
        /// <param name="lockName">Lock name. Must not be null.</param>
        /// <param name="waitSeconds">Seconds to wait. Minimum: 0.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="ArgumentNullException">Thrown when lockName is null.</exception>
        public override SqlStatement? AcquireMigrationLockSql(string lockName, int waitSeconds)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "DECLARE durable_result INTEGER; durable_cursor SYS_REFCURSOR; BEGIN " +
                "BEGIN EXECUTE IMMEDIATE 'BEGIN :r := DBMS_LOCK.REQUEST(id => :i, lockmode => 6, timeout => :t, release_on_commit => FALSE); END;' " +
                "USING OUT durable_result, IN DBMS_UTILITY.GET_HASH_VALUE(:p0, 0, 1073741824), IN :p1; " +
                LockPrivilegeHandler() +
                "OPEN durable_cursor FOR SELECT CASE WHEN durable_result IN (0, 4) THEN 1 ELSE 0 END FROM DUAL; " +
                "DBMS_SQL.RETURN_RESULT(durable_cursor); END;",
                new[] { new SqlParameterValue(":p0", lockName), new SqlParameterValue(":p1", Math.Max(0, waitSeconds)) });
        }

        /// <summary>
        /// Returns a PL/SQL block releasing the DBMS_LOCK user lock taken by <see cref="AcquireMigrationLockSql"/>.
        /// </summary>
        /// <param name="lockName">Lock name. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="ArgumentNullException">Thrown when lockName is null.</exception>
        public override SqlStatement? ReleaseMigrationLockSql(string lockName)
        {
            ArgumentNullException.ThrowIfNull(lockName);
            return new SqlStatement(
                "DECLARE durable_result INTEGER; BEGIN " +
                "BEGIN EXECUTE IMMEDIATE 'BEGIN :r := DBMS_LOCK.RELEASE(id => :i); END;' " +
                "USING OUT durable_result, IN DBMS_UTILITY.GET_HASH_VALUE(:p0, 0, 1073741824); " +
                LockPrivilegeHandler() + "END;",
                new[] { new SqlParameterValue(":p0", lockName) });
        }

        /// <summary>
        /// Writes a statement to a migration script for SQL*Plus, SQLcl or SQL Developer: PL/SQL blocks end with their own
        /// semicolon and a "/" line; other statements end with a semicolon.
        /// </summary>
        /// <param name="script">Script being written. Must not be null.</param>
        /// <param name="sql">Statement text with parameters inlined. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when script or sql is null.</exception>
        public override void AppendScriptStatement(StringBuilder script, string sql)
        {
            ArgumentNullException.ThrowIfNull(script);
            ArgumentNullException.ThrowIfNull(sql);
            string text = sql.TrimEnd();
            if (_PlSqlBlock.IsMatch(text))
            {
                script.Append(text);
                if (!text.EndsWith(";", StringComparison.Ordinal)) script.Append(';');
                script.AppendLine().AppendLine("/");
                return;
            }

            script.Append(text.TrimEnd(';')).AppendLine(";");
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Gets "ADD": Oracle's ALTER TABLE adds a column without the COLUMN keyword.
        /// </summary>
        protected override string AddColumnKeyword => "ADD";

        /// <inheritdoc />
        protected override string AutoIncrementColumnType(ColumnMetadata column, bool inlinePrimaryKey)
        {
            Type type = column.ClrType;
            string baseType = type == typeof(long) ? "NUMBER(19)" : type == typeof(short) ? "NUMBER(5)" : "NUMBER(10)";
            return baseType + " GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL" + (inlinePrimaryKey ? " PRIMARY KEY" : string.Empty);
        }

        /// <summary>
        /// Renders a binary literal (HEXTORAW('...')); an empty array is NULL, as Oracle stores it.
        /// </summary>
        /// <param name="value">Bytes. Must not be null.</param>
        /// <returns>The literal.</returns>
        protected override string BinaryLiteral(byte[] value)
        {
            return value.Length == 0 ? "NULL" : "HEXTORAW('" + Convert.ToHexString(value) + "')";
        }

        private string DictionaryName(string expression)
        {
            // With upper-case folding, a name stored in upper case came from an unquoted or folded identifier, so it is
            // reported in lower case (it folds back to the same stored name); mixed-case names are reported as stored.
            if (!UpperCaseIdentifiers) return expression;
            return "CASE WHEN " + expression + " = UPPER(" + expression + ") THEN LOWER(" + expression + ") ELSE " + expression + " END";
        }

        private static string BoundedString(int length)
        {
            return "VARCHAR2(" + length.ToString(CultureInfo.InvariantCulture) + " CHAR)";
        }

        private static bool IsLob(string? columnType, string lob)
        {
            return columnType != null && string.Equals(columnType, lob, StringComparison.OrdinalIgnoreCase);
        }

        private static string AddMonths(string value, string months)
        {
            // ADD_MONTHS returns a DATE, so the fractional seconds of the original value are added back.
            return "(CAST(ADD_MONTHS(" + value + ", " + months + ") AS TIMESTAMP(7)) + NUMTODSINTERVAL(EXTRACT(SECOND FROM " + value
                + ") - FLOOR(EXTRACT(SECOND FROM " + value + ")), 'SECOND'))";
        }

        private static string IntervalLiteral(TimeSpan span)
        {
            TimeSpan magnitude = span.Duration();
            string text = magnitude.Days.ToString(CultureInfo.InvariantCulture) + " " + magnitude.ToString("hh\\:mm\\:ss\\.fffffff", CultureInfo.InvariantCulture);
            return "INTERVAL '" + (span < TimeSpan.Zero ? "-" : "+") + text + "' DAY(9) TO SECOND(7)";
        }

        private static string LockPrivilegeHandler()
        {
            return "EXCEPTION WHEN OTHERS THEN IF SQLCODE = -6550 THEN RAISE_APPLICATION_ERROR(-20901, " +
                "'Durable migration locking calls SYS.DBMS_LOCK; run GRANT EXECUTE ON SYS.DBMS_LOCK TO ' || USER || ' as a DBA. ' || SQLERRM); " +
                "END IF; RAISE; END; ";
        }

        private string IgnoreErrorBlock(string ddl, int ignoredCode)
        {
            return "BEGIN EXECUTE IMMEDIATE " + StringLiteral(ddl) + "; EXCEPTION WHEN OTHERS THEN IF SQLCODE <> "
                + ignoredCode.ToString(CultureInfo.InvariantCulture) + " THEN RAISE; END IF; END;";
        }

        private string DictionaryFilter(string tableName, string alias, List<SqlParameterValue> parameters, string ownerColumn = "OWNER")
        {
            int dot = tableName.LastIndexOf('.');
            string owner = dot > 0 ? FoldIdentifier(tableName.Substring(0, dot)) : string.Empty;
            string table = FoldIdentifier(dot > 0 ? tableName.Substring(dot + 1) : tableName);
            string tableParameter = FormatParameterName(parameters.Count);
            parameters.Add(new SqlParameterValue(tableParameter, table));
            if (owner.Length == 0)
                return alias + "." + ownerColumn + " = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND " + alias + ".TABLE_NAME = " + tableParameter;

            string ownerParameter = FormatParameterName(parameters.Count);
            parameters.Add(new SqlParameterValue(ownerParameter, owner));
            return alias + "." + ownerColumn + " = " + ownerParameter + " AND " + alias + ".TABLE_NAME = " + tableParameter;
        }

        #endregion
    }
}
