namespace Test.Shared.Validation
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.Linq;

    /// <summary>
    /// Validates that a book title is appropriate and doesn't contain profanity
    /// </summary>
    public class ValidBookTitleAttribute : ValidationAttribute
    {
        /// <summary>
        /// Array of profanity words that are not allowed in book titles
        /// </summary>
        private readonly string[] ProfanityWords = { "damn", "hell" }; // Mild example words
        
        /// <summary>
        /// Validates the specified value
        /// </summary>
        /// <param name="value">The value to validate</param>
        /// <returns>True if the value is valid, false otherwise</returns>
        public override bool IsValid(object value)
        {
            if (value == null || !(value is string title))
                return true; // Let Required handle null validation

            // Check for basic profanity (this is a simple example)
            if (ProfanityWords.Any(word => title.Contains(word, StringComparison.OrdinalIgnoreCase)))
                return false;

            // Must not be all numbers
            if (title.All(char.IsDigit))
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
            return $"The {name} field contains inappropriate content or invalid format.";
        }
    }
}