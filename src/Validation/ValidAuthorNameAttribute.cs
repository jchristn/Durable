namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Validates that an author name doesn't contain reserved words or invalid characters
    /// </summary>
    public class ValidAuthorNameAttribute : ValidationAttribute
    {
        private readonly string[] ReservedWords = { "admin", "system", "null", "undefined", "test" };
        private readonly char[] InvalidChars = { '<', '>', '&', '"', '\'' };

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

        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field contains invalid characters or reserved words. Avoid: {string.Join(", ", ReservedWords)} and special HTML characters.";
        }
    }
}