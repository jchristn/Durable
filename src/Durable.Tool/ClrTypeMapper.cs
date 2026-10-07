namespace Durable.Tool
{
    using System;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// Chooses the CLR type of a scaffolded column from the database type reported by schema introspection. The choices
    /// mirror each dialect's <see cref="ISqlDialect.GetColumnType"/>, so a scaffolded entity maps back to the same column
    /// types (SQLite compares by type affinity, so its TEXT columns become strings).
    /// </summary>
    internal static class ClrTypeMapper
    {
        private static readonly ScaffoldColumnType _String = new ScaffoldColumnType("string", true, true, "string.Empty");
        private static readonly ScaffoldColumnType _Bytes = new ScaffoldColumnType("byte[]", true, false, "Array.Empty<byte>()");

        /// <summary>
        /// Maps a column to a CLR type.
        /// </summary>
        /// <param name="dialect">Dialect of the database. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The CLR type.</returns>
        public static ScaffoldColumnType Map(ISqlDialect dialect, ColumnSchema column)
        {
            ArgumentNullException.ThrowIfNull(dialect);
            ArgumentNullException.ThrowIfNull(column);
            string type = dialect.NormalizeColumnType(column.DataType);
            int paren = type.IndexOf('(');
            string name = (paren < 0 ? type : type.Substring(0, paren)).Trim();
            string arguments = paren < 0 ? string.Empty : type.Substring(paren);
            bool unsigned = type.Contains(" unsigned", StringComparison.Ordinal);
            name = name.Replace(" unsigned", string.Empty, StringComparison.Ordinal).Replace(" zerofill", string.Empty, StringComparison.Ordinal).Trim();

            ScaffoldColumnType? mapped = null;
            RepositoryType repository = dialect.RepositoryType;
            if (repository == RepositoryType.Sqlite) mapped = MapSqlite(name);
            else if (repository == RepositoryType.Postgres) mapped = MapPostgres(name, arguments);
            else if (repository == RepositoryType.MySql) mapped = MapMySql(name, arguments, unsigned);
            else if (repository == RepositoryType.SqlServer) mapped = MapSqlServer(name, arguments);
            else if (repository == RepositoryType.Oracle) mapped = MapOracle(name, arguments);

            return mapped ?? new ScaffoldColumnType("string", true, true, "string.Empty", "Database type " + column.DataType + " has no direct CLR mapping; review this property.");
        }

        private static ScaffoldColumnType? MapSqlite(string affinity)
        {
            switch (affinity)
            {
                case "integer": return Value("long");
                case "real": return Value("double");
                case "numeric": return Value("decimal");
                case "blob": return _Bytes;
                case "text": return _String;
                default: return null;
            }
        }

        private static ScaffoldColumnType? MapPostgres(string name, string arguments)
        {
            switch (name)
            {
                case "smallint": return Value("short");
                case "integer": return Value("int");
                case "bigint": return Value("long");
                case "boolean":
                case "bool": return Value("bool");
                case "real": return Value("float");
                case "double precision": return Value("double");
                case "numeric":
                case "decimal":
                case "money": return Value("decimal");
                case "timestamp": return Value("DateTime");
                case "timestamptz": return Value("DateTimeOffset");
                case "date": return Value("DateOnly");
                case "time": return Value("TimeOnly");
                case "interval": return Value("TimeSpan");
                case "uuid": return Value("Guid");
                case "bytea": return _Bytes;
                case "char": return arguments == "(1)" || arguments.Length == 0 ? Value("char") : _String;
                case "varchar":
                case "text":
                case "citext":
                case "name":
                case "xml":
                case "json":
                case "jsonb": return _String;
                default: return null;
            }
        }

        private static ScaffoldColumnType? MapMySql(string name, string arguments, bool unsigned)
        {
            switch (name)
            {
                case "tinyint": return arguments == "(1)" ? Value("bool") : Value(unsigned ? "byte" : "sbyte");
                case "bit": return arguments == "(1)" || arguments.Length == 0 ? Value("bool") : Value("ulong");
                case "smallint": return Value(unsigned ? "ushort" : "short");
                case "mediumint":
                case "int": return Value(unsigned ? "uint" : "int");
                case "bigint": return Value(unsigned ? "ulong" : "long");
                case "year": return Value("short");
                case "float": return Value("float");
                case "double": return Value("double");
                case "decimal": return Value("decimal");
                case "datetime":
                case "timestamp": return Value("DateTime");
                case "date": return Value("DateOnly");
                case "time": return Value("TimeOnly");
                case "char": return arguments == "(36)" ? Value("Guid") : arguments == "(1)" ? Value("char") : _String;
                case "varchar":
                case "tinytext":
                case "text":
                case "mediumtext":
                case "longtext":
                case "enum":
                case "set":
                case "json": return _String;
                case "binary":
                case "varbinary":
                case "tinyblob":
                case "blob":
                case "mediumblob":
                case "longblob": return _Bytes;
                default: return null;
            }
        }

        private static ScaffoldColumnType? MapSqlServer(string name, string arguments)
        {
            switch (name)
            {
                case "bit": return Value("bool");
                case "tinyint": return Value("byte");
                case "smallint": return Value("short");
                case "int": return Value("int");
                case "bigint": return Value("long");
                case "real": return Value("float");
                case "float": return Value("double");
                case "decimal":
                case "money":
                case "smallmoney": return Value("decimal");
                case "datetime2":
                case "datetime":
                case "smalldatetime": return Value("DateTime");
                case "datetimeoffset": return Value("DateTimeOffset");
                case "date": return Value("DateOnly");
                case "time": return Value("TimeOnly");
                case "uniqueidentifier": return Value("Guid");
                case "nchar":
                case "char": return arguments == "(1)" ? Value("char") : _String;
                case "nvarchar":
                case "varchar":
                case "ntext":
                case "text":
                case "xml":
                case "sysname": return _String;
                case "binary":
                case "varbinary":
                case "image":
                case "timestamp": return _Bytes;
                default: return null;
            }
        }

        private static ScaffoldColumnType? MapOracle(string name, string arguments)
        {
            switch (name)
            {
                case "number":
                    {
                        // Mirrors OracleDialect.GetColumnType: NUMBER(1) bool, (3) byte, (5) short, (10) int, (19) long.
                        if (arguments.Contains(',', StringComparison.Ordinal) || arguments.Length == 0) return Value("decimal");
                        switch (arguments)
                        {
                            case "(1)": return Value("bool");
                            case "(3)": return Value("byte");
                            case "(5)": return Value("short");
                            case "(10)": return Value("int");
                            case "(19)": return Value("long");
                            default: return Value("decimal");
                        }
                    }

                case "integer":
                case "int":
                case "smallint": return Value("decimal");
                case "binary_float": return Value("float");
                case "binary_double":
                case "float": return Value("double");
                case "timestamp": return arguments.EndsWith(" with time zone", StringComparison.Ordinal) ? Value("DateTimeOffset") : Value("DateTime");
                case "date": return Value("DateTime");
                case "interval day": return arguments.StartsWith("(0)", StringComparison.Ordinal) ? Value("TimeOnly") : Value("TimeSpan");
                case "raw": return arguments == "(16)" ? Value("Guid") : _Bytes;
                case "blob":
                case "long raw": return _Bytes;
                case "varchar2":
                case "nvarchar2":
                case "char":
                case "nchar":
                    return arguments == "(1 char)" || arguments == "(1)" || arguments == "(1 byte)" ? Value("char") : _String;
                case "clob":
                case "nclob":
                case "long":
                case "json": return _String;
                default: return null;
            }
        }

        private static ScaffoldColumnType Value(string typeName)
        {
            return new ScaffoldColumnType(typeName, false, false, null);
        }
    }
}
