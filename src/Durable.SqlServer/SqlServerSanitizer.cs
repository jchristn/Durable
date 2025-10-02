namespace Durable.SqlServer
{
    using System;
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>
    /// SQL Server-specific implementation of ISanitizer that provides secure sanitization
    /// of values to prevent SQL injection attacks.
    /// Uses SQL Server-specific syntax including square bracket identifiers and T-SQL functions.
    /// </summary>
    public class SqlServerSanitizer : ISanitizer
    {

        #region Public-Members

        #endregion

        #region Private-Members

        private static readonly Regex _SqlIdentifierPattern = new Regex(@"^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled);

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Sanitizes a string value for safe insertion into SQL queries.
        /// Uses proper SQL Server escaping to prevent injection attacks.
        /// </summary>
        /// <param name="value">The string value to sanitize</param>
        /// <returns>A sanitized string safe for SQL insertion</returns>
        public string SanitizeString(string value)
        {
            if (value == null) return "NULL";

            // SQL Server-specific escaping - use single quote doubling
            StringBuilder escaped = new StringBuilder();
            foreach (char c in value)
            {
                switch (c)
                {
                    case '\'':
                        escaped.Append("''");  // Escape single quotes by doubling
                        break;
                    case '\0':
                        // SQL Server doesn't allow null bytes in strings
                        throw new ArgumentException("String values cannot contain null bytes in SQL Server");
                    case '\b':
                        escaped.Append("\\b"); // Escape backspace
                        break;
                    case '\n':
                        escaped.Append("\\n"); // Escape newline
                        break;
                    case '\r':
                        escaped.Append("\\r"); // Escape carriage return
                        break;
                    case '\t':
                        escaped.Append("\\t"); // Escape tab
                        break;
                    default:
                        escaped.Append(c);
                        break;
                }
            }

            return $"'{escaped.ToString()}'";
        }

        /// <summary>
        /// Sanitizes a string value for use in LIKE operations.
        /// Handles both SQL injection prevention and LIKE special characters.
        /// SQL Server uses square brackets as default escape for LIKE patterns.
        /// </summary>
        /// <param name="value">The string value to sanitize for LIKE operations</param>
        /// <returns>A sanitized string safe for LIKE operations</returns>
        public string SanitizeLikeValue(string value)
        {
            if (value == null) return "NULL";

            // First escape SQL injection characters
            string escaped = SanitizeString(value);

            // Remove the outer quotes to process LIKE characters
            escaped = escaped.Substring(1, escaped.Length - 2);

            // Escape LIKE special characters using square brackets
            // SQL Server LIKE patterns: % (any characters), _ (single character), [] (character set)
            escaped = escaped.Replace("[", "[[]");  // Escape existing brackets first
            escaped = escaped.Replace("%", "[%]");  // Escape percent signs
            escaped = escaped.Replace("_", "[_]");  // Escape underscores

            return $"'{escaped}'";
        }

        /// <summary>
        /// Sanitizes an identifier (table name, column name, etc.) for safe insertion into SQL.
        /// Uses SQL Server square bracket notation for identifiers that might contain special characters.
        /// </summary>
        /// <param name="identifier">The identifier to sanitize</param>
        /// <returns>A sanitized identifier safe for SQL insertion</returns>
        public string SanitizeIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                throw new ArgumentException("Identifier cannot be null or empty", nameof(identifier));

            // Check if identifier contains only safe characters and is not a SQL Server reserved word
            if (_SqlIdentifierPattern.IsMatch(identifier) && !IsReservedWord(identifier.ToLowerInvariant()))
            {
                return identifier;
            }

            // Use SQL Server square bracket notation for identifiers with special characters or reserved words
            string escaped = identifier.Replace("]", "]]");
            return $"[{escaped}]";
        }

        /// <summary>
        /// Determines if a value requires sanitization based on its type.
        /// Safe types like Guid, numeric types, etc. don't need string sanitization.
        /// </summary>
        /// <param name="value">The value to check</param>
        /// <returns>True if the value needs sanitization, false otherwise</returns>
        public bool RequiresSanitization(object value)
        {
            if (value == null) return false;

            Type type = value.GetType();

            // Safe numeric types
            if (type.IsPrimitive && type != typeof(char))
                return false;

            // Safe specific types
            if (type == typeof(Guid) ||
                type == typeof(decimal) ||
                type == typeof(DateTime) ||
                type == typeof(DateTimeOffset) ||
                type == typeof(TimeSpan))
                return false;

            // Nullable versions of safe types
            Type? underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                return RequiresSanitization(Activator.CreateInstance(underlyingType) ?? false);

            // Everything else (strings, objects, etc.) needs sanitization
            return true;
        }

        /// <summary>
        /// Formats a value for safe SQL insertion, applying sanitization as needed.
        /// This is the main method that should be used for formatting any value.
        /// Handles SQL Server-specific types including UNIQUEIDENTIFIER, BIT, DATETIME2, and binary types.
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <returns>A safely formatted value for SQL insertion</returns>
        public string FormatValue(object value)
        {
            return value switch
            {
                null => "NULL",
                string s => SanitizeString(s),
                bool b => b ? "1" : "0", // SQL Server uses BIT type (0/1)
                DateTime dt => FormatDateTime(dt),
                DateTimeOffset dto => FormatDateTimeOffset(dto),
                DateOnly dateOnly => SanitizeString(dateOnly.ToString("yyyy-MM-dd")),
                TimeOnly timeOnly => SanitizeString(timeOnly.ToString("HH:mm:ss.fffffff")),
                TimeSpan ts => SanitizeString(ts.ToString()),
                Guid guid => FormatGuid(guid),
                char c => SanitizeString(c.ToString()),
                byte[] bytes => FormatByteArray(bytes),
                _ when !RequiresSanitization(value) => value.ToString() ?? "NULL",
                _ => SanitizeString(value.ToString() ?? "")
            };
        }

        /// <summary>
        /// Formats a GUID for SQL Server UNIQUEIDENTIFIER type.
        /// </summary>
        /// <param name="guid">The GUID to format</param>
        /// <returns>A formatted UNIQUEIDENTIFIER string</returns>
        public string FormatGuid(Guid guid)
        {
            return $"'{guid:D}'";
        }

        /// <summary>
        /// Formats a DateTime for SQL Server datetime2 insertion.
        /// SQL Server datetime2 has higher precision than datetime.
        /// </summary>
        /// <param name="dateTime">The DateTime to format</param>
        /// <returns>A formatted datetime2 string</returns>
        public string FormatDateTime(DateTime dateTime)
        {
            // SQL Server datetime2 supports up to 7 decimal places for fractional seconds
            // Format: YYYY-MM-DD HH:MM:SS.FFFFFFF
            return $"'{dateTime:yyyy-MM-dd HH:mm:ss.fffffff}'";
        }

        /// <summary>
        /// Formats a DateTimeOffset for SQL Server datetimeoffset insertion.
        /// </summary>
        /// <param name="dateTimeOffset">The DateTimeOffset to format</param>
        /// <returns>A formatted datetimeoffset string</returns>
        public string FormatDateTimeOffset(DateTimeOffset dateTimeOffset)
        {
            // SQL Server datetimeoffset format includes timezone offset
            // Format: YYYY-MM-DD HH:MM:SS.FFFFFFF +/-HH:MM
            return $"'{dateTimeOffset:yyyy-MM-dd HH:mm:ss.fffffffzzz}'";
        }

        /// <summary>
        /// Formats a byte array for SQL Server varbinary insertion.
        /// SQL Server uses 0x prefix for binary literals.
        /// </summary>
        /// <param name="bytes">The byte array to format</param>
        /// <returns>A formatted binary literal for SQL Server</returns>
        public string FormatByteArray(byte[] bytes)
        {
            if (bytes == null) return "NULL";
            if (bytes.Length == 0) return "0x";

            StringBuilder hex = new StringBuilder("0x", bytes.Length * 2 + 2);
            foreach (byte b in bytes)
            {
                hex.AppendFormat("{0:X2}", b);
            }

            return hex.ToString();
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Checks if the given identifier is a SQL Server reserved word that requires quoting.
        /// </summary>
        /// <param name="identifier">The identifier to check (should be lowercase)</param>
        /// <returns>True if the identifier is a reserved word</returns>
        private static bool IsReservedWord(string identifier)
        {
            // Common SQL Server reserved words that would cause issues if unquoted
            // This is not exhaustive but covers the most problematic ones
            string[] reservedWords = {
                "add", "all", "alter", "and", "any", "as", "asc", "authorization", "backup",
                "begin", "between", "break", "browse", "bulk", "by", "cascade", "case", "check",
                "checkpoint", "close", "clustered", "coalesce", "collate", "column", "commit",
                "compute", "constraint", "contains", "containstable", "continue", "convert",
                "create", "cross", "current", "current_date", "current_time", "current_timestamp",
                "current_user", "cursor", "database", "dbcc", "deallocate", "declare", "default",
                "delete", "deny", "desc", "disk", "distinct", "distributed", "double", "drop",
                "dump", "else", "end", "errlvl", "escape", "except", "exec", "execute", "exists",
                "exit", "external", "fetch", "file", "fillfactor", "for", "foreign", "freetext",
                "freetexttable", "from", "full", "function", "goto", "grant", "group", "having",
                "holdlock", "identity", "identity_insert", "identitycol", "if", "in", "index",
                "inner", "insert", "intersect", "into", "is", "join", "key", "kill", "left",
                "like", "lineno", "load", "merge", "national", "nocheck", "nonclustered", "not",
                "null", "nullif", "of", "off", "offsets", "on", "open", "opendatasource",
                "openquery", "openrowset", "openxml", "option", "or", "order", "outer", "over",
                "percent", "pivot", "plan", "precision", "primary", "print", "proc", "procedure",
                "public", "raiserror", "read", "readtext", "reconfigure", "references", "replication",
                "restore", "restrict", "return", "revert", "revoke", "right", "rollback", "rowcount",
                "rowguidcol", "rule", "save", "schema", "securityaudit", "select", "semantickeyphrasetable",
                "semanticsimilaritydetailstable", "semanticsimilaritytable", "session_user", "set",
                "setuser", "shutdown", "some", "statistics", "system_user", "table", "tablesample",
                "textsize", "then", "to", "top", "tran", "transaction", "trigger", "truncate",
                "try_convert", "tsequal", "union", "unique", "unpivot", "update", "updatetext",
                "use", "user", "values", "varying", "view", "waitfor", "when", "where", "while",
                "with", "within", "writetext"
            };

            return Array.BinarySearch(reservedWords, identifier) >= 0;
        }

        #endregion
    }
}
