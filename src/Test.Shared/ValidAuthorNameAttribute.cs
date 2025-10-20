namespace Test.Shared.Validation
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;

    /// <summary>
    /// Validates that an author name doesn't contain reserved words or invalid characters
    /// </summary>
    public class ValidAuthorNameAttribute : ValidationAttribute
    {
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

        #region Private-Members

        /// <summary>
        /// Array of reserved words that are not allowed in author names
        /// </summary>
        private readonly string[] ReservedWords = { "admin", "system", "null", "undefined", "test" };
        
        /// <summary>
        /// Array of invalid characters that are not allowed in author names
        /// </summary>
        private readonly char[] InvalidChars = { '<', '>', '&', '"', '\'' };

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ValidAuthorNameAttribute"/> class.
        /// </summary>
        public ValidAuthorNameAttribute()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates the specified value
        /// </summary>
        /// <param name="value">The value to validate</param>
        /// <returns>True if the value is valid, false otherwise</returns>
        public override bool IsValid(object value)
        {
            if (value == null || !(value is string name))
                return true; // Let Required handle null validation

            // Check for reserved words
            if (ReservedWords.Any(word => name.Equals(word, StringComparison.OrdinalIgnoreCase)))
                return false;

            // Check for invalid characters
            if (InvalidChars.Any(ch => name.Contains(ch)))
                return false;

            // Check for all whitespace
            if (string.IsNullOrWhiteSpace(name))
                return false;

            return true;
        }

        /// <summary>
        /// Formats the error message for validation failure
        /// </summary>
        /// <param name="name">The name of the field being validated</param>
        /// <returns>The formatted error message</returns>
        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field contains invalid characters or reserved words. Avoid: {string.Join(", ", ReservedWords)} and special HTML characters.";
        }

        #endregion

#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    }
}