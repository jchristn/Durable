namespace Durable
{
    /// <summary>
    /// Interface for sanitizing values that will be inserted into SQL queries.
    /// Provides methods to safely format different types of values to prevent SQL injection.
    /// </summary>
    public interface ISanitizer
    {
        /// <summary>
        /// Sanitizes a string value for safe insertion into SQL queries.
        /// This method should handle SQL injection prevention for string values.
        /// </summary>
        /// <param name="value">The string value to sanitize</param>
        /// <returns>A sanitized string safe for SQL insertion</returns>
        string SanitizeString(string value);

        /// <summary>
        /// Sanitizes a string value for use in LIKE operations.
        /// This method should handle SQL injection prevention and LIKE special characters.
        /// </summary>
        /// <param name="value">The string value to sanitize for LIKE operations</param>
        /// <returns>A sanitized string safe for LIKE operations</returns>
        string SanitizeLikeValue(string value);

        /// <summary>
        /// Sanitizes an identifier (table name, column name, etc.) for safe insertion into SQL.
        /// This method should handle SQL injection prevention for SQL identifiers.
        /// </summary>
        /// <param name="identifier">The identifier to sanitize</param>
        /// <returns>A sanitized identifier safe for SQL insertion</returns>
        string SanitizeIdentifier(string identifier);

        /// <summary>
        /// Determines if a value requires sanitization based on its type.
        /// Values like Guid, numbers, etc. may not need string sanitization.
        /// </summary>
        /// <param name="value">The value to check</param>
        /// <returns>True if the value needs sanitization, false otherwise</returns>
        bool RequiresSanitization(object value);

        /// <summary>
        /// Formats a value for safe SQL insertion, applying sanitization as needed.
        /// This is the main method that should be used for formatting any value.
        /// </summary>
        /// <param name="value">The value to format</param>
        /// <returns>A safely formatted value for SQL insertion</returns>
        string FormatValue(object value);
    }
}