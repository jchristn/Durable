namespace Durable.Postgres
{
    using System;
    using System.Text.Json;
    using System.Globalization;
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>
    /// PostgreSQL-specific implementation of ISanitizer that provides secure sanitization
    /// of values to prevent SQL injection attacks.
    /// </summary>
    public class PostgresSanitizer : ISanitizer
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
        /// Uses proper PostgreSQL escaping to prevent injection attacks.
        /// </summary>
        /// <param name="value">The string value to sanitize</param>
        /// <returns>A sanitized string safe for SQL insertion</returns>
        public string SanitizeString(string value)
        {
            if (value == null) return "NULL";

            // PostgreSQL-specific escaping - use single quote doubling for string literals
            StringBuilder escaped = new StringBuilder();
            foreach (char c in value)
            {
                switch (c)
                {
                    case '\'':
                        escaped.Append("''");  // Escape single quotes by doubling (PostgreSQL standard)
                        break;
                    case '\\':
                        escaped.Append("\\\\"); // Escape backslashes in standard_conforming_strings = off mode
                        break;
                    case '\0':
                        // PostgreSQL doesn't allow null bytes in strings
                        throw new ArgumentException("String values cannot contain null bytes in PostgreSQL");
                    case '\b':
                        escaped.Append("\\b"); // Escape backspace
                        break;
                    case '\f':
                        escaped.Append("\\f"); // Escape form feed
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
                    case '\v':
                        escaped.Append("\\v"); // Escape vertical tab
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

            // Escape LIKE special characters - PostgreSQL uses backslash as default escape
            escaped = escaped.Replace("\\", "\\\\"); // Escape existing backslashes first
            escaped = escaped.Replace("%", "\\%");   // Escape percent signs
            escaped = escaped.Replace("_", "\\_");   // Escape underscores

            return $"'{escaped}'";
        }

        /// <summary>
        /// Sanitizes an identifier (table name, column name, etc.) for safe insertion into SQL.
        /// Uses PostgreSQL double quote notation for identifiers that might contain special characters.
        /// </summary>
        /// <param name="identifier">The identifier to sanitize</param>
        /// <returns>A sanitized identifier safe for SQL insertion</returns>
        public string SanitizeIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                throw new ArgumentException("Identifier cannot be null or empty", nameof(identifier));

            // Check if identifier contains only safe characters and is not a PostgreSQL reserved word
            if (_SqlIdentifierPattern.IsMatch(identifier) && !IsReservedWord(identifier.ToLowerInvariant()))
            {
                return identifier.ToLowerInvariant(); // PostgreSQL folds unquoted identifiers to lowercase
            }

            // Use PostgreSQL double quote notation for identifiers with special characters or reserved words
            string escaped = identifier.Replace("\"", "\"\"");
            return $"\"{escaped}\"";
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
        /// Handles PostgreSQL-specific types including arrays, JSON, UUID, timestamps, and geometric types.
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <param name="propertyInfo">Optional property information for attribute-based formatting hints.</param>
        /// <returns>A safely formatted value for SQL insertion</returns>
        public string FormatValue(object value, System.Reflection.PropertyInfo? propertyInfo = null)
        {
            return value switch
            {
                null => "NULL",
                string s => SanitizeString(s),
                bool b => b ? "true" : "false", // PostgreSQL uses true/false instead of 1/0
                Enum e => SanitizeString(e.ToString()),
                DateTime dt => FormatDateTime(dt),
                DateTimeOffset dto => FormatDateTimeOffset(dto),
                DateOnly dateOnly => SanitizeString(dateOnly.ToString("yyyy-MM-dd")),
                TimeOnly timeOnly => SanitizeString(timeOnly.ToString("HH:mm:ss.fff")),
                TimeSpan ts => SanitizeString(ts.ToString()),
                Guid guid => FormatGuid(guid),
                char c => SanitizeString(c.ToString()),
                byte[] bytes => FormatByteArray(bytes),
                Array array => FormatArray(array),
                System.Net.IPAddress ip => SanitizeString(ip.ToString()),
                _ when value.GetType().IsArray => FormatArray((Array)value),
                _ when value.GetType().Namespace?.Contains("NpgsqlTypes") == true => FormatNpgsqlType(value),
                _ when !RequiresSanitization(value) => value.ToString() ?? "NULL",
                _ => SanitizeString(value.ToString() ?? "")
            };
        }

        /// <summary>
        /// Formats a PostgreSQL array for SQL insertion.
        /// Supports multi-dimensional arrays and proper PostgreSQL array literal syntax.
        /// </summary>
        /// <param name="array">The array to format</param>
        /// <returns>A PostgreSQL array literal string</returns>
        public string FormatArray(Array array)
        {
            if (array == null) return "NULL";

            StringBuilder sb = new StringBuilder();
            sb.Append("ARRAY[");

            bool first = true;
            foreach (object? item in array)
            {
                if (!first) sb.Append(',');
                sb.Append(FormatValue(item));
                first = false;
            }

            sb.Append(']');
            return sb.ToString();
        }

        /// <summary>
        /// Formats a JSON object or string for PostgreSQL insertion.
        /// </summary>
        /// <param name="jsonValue">The JSON value to format</param>
        /// <param name="isJsonB">Whether to format as JSONB (true) or JSON (false)</param>
        /// <returns>A formatted JSON value for SQL insertion</returns>
        public string FormatJson(object jsonValue, bool isJsonB = false)
        {
            if (jsonValue == null) return "NULL";

            string jsonString = jsonValue switch
            {
                string s => s,
                _ => JsonSerializer.Serialize(jsonValue)
            };

            string formattedJson = SanitizeString(jsonString);
            return isJsonB ? $"{formattedJson}::jsonb" : $"{formattedJson}::json";
        }

        /// <summary>
        /// Formats a GUID for PostgreSQL UUID type.
        /// </summary>
        /// <param name="guid">The GUID to format</param>
        /// <returns>A formatted UUID string</returns>
        public string FormatGuid(Guid guid)
        {
            return $"'{guid:D}'::uuid";
        }

        /// <summary>
        /// Formats a DateTime for PostgreSQL timestamp insertion.
        /// </summary>
        /// <param name="dateTime">The DateTime to format</param>
        /// <returns>A formatted timestamp string</returns>
        public string FormatDateTime(DateTime dateTime)
        {
            if (dateTime.Kind == DateTimeKind.Utc)
            {
                return $"'{dateTime:yyyy-MM-dd HH:mm:ss.fffffff}'::timestamp";
            }
            else
            {
                return $"'{dateTime:yyyy-MM-dd HH:mm:ss.fffffff}'::timestamptz";
            }
        }

        /// <summary>
        /// Formats a DateTimeOffset for PostgreSQL timestamptz insertion.
        /// </summary>
        /// <param name="dateTimeOffset">The DateTimeOffset to format</param>
        /// <returns>A formatted timestamptz string</returns>
        public string FormatDateTimeOffset(DateTimeOffset dateTimeOffset)
        {
            return $"'{dateTimeOffset:yyyy-MM-dd HH:mm:ss.fffK}'::timestamptz";
        }

        /// <summary>
        /// Formats PostgreSQL-specific NpgsqlTypes for SQL insertion.
        /// </summary>
        /// <param name="value">The NpgsqlTypes value to format</param>
        /// <returns>A formatted value string</returns>
        public string FormatNpgsqlType(object value)
        {
            if (value == null) return "NULL";

            // Handle PostgreSQL geometric types, network types, ranges, etc.
            string typeName = value.GetType().Name.ToLowerInvariant();
            string valueString = value.ToString() ?? "";

            return typeName switch
            {
                string s when s.Contains("point") => $"'{valueString}'::point",
                string s when s.Contains("line") => $"'{valueString}'::line",
                string s when s.Contains("box") => $"'{valueString}'::box",
                string s when s.Contains("circle") => $"'{valueString}'::circle",
                string s when s.Contains("polygon") => $"'{valueString}'::polygon",
                string s when s.Contains("path") => $"'{valueString}'::path",
                string s when s.Contains("inet") => $"'{valueString}'::inet",
                string s when s.Contains("cidr") => $"'{valueString}'::cidr",
                string s when s.Contains("macaddr") => $"'{valueString}'::macaddr",
                string s when s.Contains("range") => $"'{valueString}'",
                _ => SanitizeString(valueString)
            };
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Checks if the given identifier is a PostgreSQL reserved word that requires quoting.
        /// </summary>
        /// <param name="identifier">The identifier to check (should be lowercase)</param>
        /// <returns>True if the identifier is a reserved word</returns>
        private static bool IsReservedWord(string identifier)
        {
            // Common PostgreSQL reserved words that would cause issues if unquoted
            // This is not exhaustive but covers the most problematic ones
            string[] reservedWords = {
                "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
                "both", "case", "cast", "check", "collate", "column", "constraint",
                "create", "current_catalog", "current_date", "current_role", "current_time",
                "current_timestamp", "current_user", "default", "deferrable", "desc",
                "distinct", "do", "else", "end", "except", "false", "fetch", "for",
                "foreign", "from", "grant", "group", "having", "in", "initially",
                "intersect", "into", "leading", "limit", "localtime", "localtimestamp",
                "not", "null", "offset", "on", "only", "or", "order", "placing",
                "primary", "references", "returning", "select", "session_user", "some",
                "symmetric", "table", "then", "to", "trailing", "true", "union",
                "unique", "user", "using", "variadic", "when", "where", "window", "with"
            };

            return Array.BinarySearch(reservedWords, identifier) >= 0;
        }

        /// <summary>
        /// Formats a byte array for PostgreSQL bytea insertion.
        /// </summary>
        /// <param name="bytes">The byte array to format</param>
        /// <returns>A formatted bytea literal for PostgreSQL</returns>
        private string FormatByteArray(byte[] bytes)
        {
            if (bytes == null) return "NULL";
            if (bytes.Length == 0) return "'\\x'";

            StringBuilder hex = new StringBuilder("'\\x", bytes.Length * 2 + 3);
            foreach (byte b in bytes)
            {
                hex.AppendFormat("{0:x2}", b);
            }
            hex.Append("'");

            return hex.ToString();
        }

        #endregion
    }
}