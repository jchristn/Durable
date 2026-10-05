namespace Durable.Sql
{
    using System;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Validates user-supplied identifiers and keyword fragments that cannot be parameterized.
    /// Thread safety: stateless.
    /// </summary>
    public static class SqlIdentifierValidator
    {
        private static readonly Regex _Identifier = new Regex("^[A-Za-z_][A-Za-z0-9_]{0,127}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex _FrameBound = new Regex("^(UNBOUNDED PRECEDING|UNBOUNDED FOLLOWING|CURRENT ROW|[0-9]{1,9} PRECEDING|[0-9]{1,9} FOLLOWING)$", RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

        /// <summary>
        /// Throws unless the value is a simple identifier (letters, digits, underscore; not starting with a digit; at most 128 characters).
        /// </summary>
        /// <param name="value">Value to check.</param>
        /// <param name="parameterName">Parameter name for the exception.</param>
        /// <returns>The value.</returns>
        /// <exception cref="ArgumentException">Thrown when the value is not a simple identifier.</exception>
        public static string RequireIdentifier(string? value, string parameterName)
        {
            if (value == null || !_Identifier.IsMatch(value))
                throw new ArgumentException("'" + value + "' is not a valid identifier (letters, digits and underscores only).", parameterName);
            return value;
        }

        /// <summary>
        /// Throws unless the value is a window frame bound such as "UNBOUNDED PRECEDING", "CURRENT ROW" or "3 FOLLOWING".
        /// </summary>
        /// <param name="value">Value to check.</param>
        /// <param name="parameterName">Parameter name for the exception.</param>
        /// <returns>The normalized (upper-case) bound.</returns>
        /// <exception cref="ArgumentException">Thrown when the value is not a valid frame bound.</exception>
        public static string RequireFrameBound(string? value, string parameterName)
        {
            if (value == null || !_FrameBound.IsMatch(value.Trim()))
                throw new ArgumentException("'" + value + "' is not a valid window frame bound.", parameterName);
            return value.Trim().ToUpperInvariant();
        }
    }
}
