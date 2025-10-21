namespace Durable.Sqlite
{
    using System;
    using System.Globalization;
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>
    /// SQLite-specific implementation of ISanitizer that provides secure sanitization
    /// of values to prevent SQL injection attacks.
    /// </summary>
    public class SqliteSanitizer : ISanitizer
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
        /// Uses proper SQL escaping to prevent injection attacks.
        /// </summary>
        /// <param name="value">The string value to sanitize</param>
        /// <returns>A sanitized string safe for SQL insertion</returns>
        public string SanitizeString(string value)
        {
            if (value == null) return "NULL";
            
            // Escape single quotes by doubling them (SQL standard)
            string escaped = value.Replace("'", "''");
            
            // Additional SQLite-specific escaping for backslashes and null bytes
            escaped = escaped.Replace("\\", "\\\\");
            escaped = escaped.Replace("\0", "");
            
            return $"'{escaped}'";
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
            string escaped = value.Replace("'", "''");
            escaped = escaped.Replace("\\", "\\\\");
            escaped = escaped.Replace("\0", "");
            
            // Escape LIKE special characters
            escaped = escaped.Replace("%", "\\%");
            escaped = escaped.Replace("_", "\\_");
            escaped = escaped.Replace("[", "\\[");
            
            return $"'{escaped}'";
        }

        /// <summary>
        /// Sanitizes an identifier (table name, column name, etc.) for safe insertion into SQL.
        /// Uses SQLite bracket notation for identifiers that might contain special characters.
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
            
            // Use SQLite bracket notation for identifiers with special characters
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
            Type underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                return RequiresSanitization(Activator.CreateInstance(underlyingType));
            
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
                Enum e => SanitizeString(e.ToString()),
                Guid g => SanitizeString(g.ToString()),
                DateTime dt => SanitizeString(dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)),
                DateTimeOffset dto => SanitizeString(dto.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)),
                TimeSpan ts => SanitizeString(ts.ToString()),
                char c => SanitizeString(c.ToString()),
                _ when !RequiresSanitization(value) => value.ToString(),
                _ => SanitizeString(value.ToString())
            };
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}