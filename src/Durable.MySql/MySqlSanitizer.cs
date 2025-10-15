namespace Durable.MySql
{
    using System;
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>
    /// MySQL-specific implementation of ISanitizer that provides secure sanitization
    /// of values to prevent SQL injection attacks.
    /// </summary>
    public class MySqlSanitizer : ISanitizer
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
        /// Uses proper MySQL escaping to prevent injection attacks.
        /// </summary>
        /// <param name="value">The string value to sanitize</param>
        /// <returns>A sanitized string safe for SQL insertion</returns>
        public string SanitizeString(string value)
        {
            if (value == null) return "NULL";

            // MySQL-specific escaping
            StringBuilder escaped = new StringBuilder();
            foreach (char c in value)
            {
                switch (c)
                {
                    case '\'':
                        escaped.Append("''");  // Escape single quotes by doubling
                        break;
                    case '\\':
                        escaped.Append("\\\\"); // Escape backslashes
                        break;
                    case '\0':
                        escaped.Append("\\0"); // Escape null bytes
                        break;
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
                    case '\u001A':
                        escaped.Append("\\Z"); // Escape substitute character
                        break;
                    case '"':
                        escaped.Append("\\\""); // Escape double quotes
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

            // Escape LIKE special characters
            escaped = escaped.Replace("%", "\\%");
            escaped = escaped.Replace("_", "\\_");

            return $"'{escaped}'";
        }

        /// <summary>
        /// Sanitizes an identifier (table name, column name, etc.) for safe insertion into SQL.
        /// Uses MySQL backtick notation for identifiers that might contain special characters.
        /// </summary>
        /// <param name="identifier">The identifier to sanitize</param>
        /// <returns>A sanitized identifier safe for SQL insertion</returns>
        public string SanitizeIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                throw new ArgumentException("Identifier cannot be null or empty", nameof(identifier));

            // Check if identifier contains only safe characters
            if (_SqlIdentifierPattern.IsMatch(identifier))
            {
                return identifier;
            }

            // Use MySQL backtick notation for identifiers with special characters
            string escaped = identifier.Replace("`", "``");
            return $"`{escaped}`";
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
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <returns>A safely formatted value for SQL insertion</returns>
        public string FormatValue(object value)
        {
            return value switch
            {
                null => "NULL",
                string s => SanitizeString(s),
                bool b => b ? "1" : "0",
                Guid g => SanitizeString(g.ToString()), // GUIDs must be quoted for VARCHAR columns
                DateTime dt => SanitizeString(dt.ToString("yyyy-MM-dd HH:mm:ss")),
                DateTimeOffset dto => SanitizeString(dto.ToString("yyyy-MM-dd HH:mm:ss")),
                TimeSpan ts => SanitizeString(ts.ToString()),
                char c => SanitizeString(c.ToString()),
                _ when !RequiresSanitization(value) => value.ToString() ?? "NULL",
                _ => SanitizeString(value.ToString() ?? "")
            };
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}