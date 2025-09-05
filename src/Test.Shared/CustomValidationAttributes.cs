using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;

namespace Test.Shared.Validation
{
    /// <summary>
    /// Validates that a company ID exists and is positive when provided
    /// </summary>
    public class ValidCompanyIdAttribute : ValidationAttribute
    {
        public override bool IsValid(object value)
        {
            if (value == null)
                return true; // Null is allowed for optional company references
            
            if (value is int companyId)
            {
                return companyId > 0;
            }
            
            return false;
        }

        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field must be a positive number when specified.";
        }
    }

    /// <summary>
    /// Validates that an author name doesn't contain reserved words or invalid characters
    /// </summary>
    public class ValidAuthorNameAttribute : ValidationAttribute
    {
        private readonly string[] _reservedWords = { "admin", "system", "null", "undefined", "test" };
        private readonly char[] _invalidChars = { '<', '>', '&', '"', '\'' };

        public override bool IsValid(object value)
        {
            if (value == null || !(value is string name))
                return true; // Let Required handle null validation

            // Check for reserved words
            if (_reservedWords.Any(word => name.Equals(word, StringComparison.OrdinalIgnoreCase)))
                return false;

            // Check for invalid characters
            if (_invalidChars.Any(ch => name.Contains(ch)))
                return false;

            // Check for all whitespace
            if (string.IsNullOrWhiteSpace(name))
                return false;

            return true;
        }

        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field contains invalid characters or reserved words. Avoid: {string.Join(", ", _reservedWords)} and special HTML characters.";
        }
    }

    /// <summary>
    /// Validates that a book title is appropriate and doesn't contain profanity
    /// </summary>
    public class ValidBookTitleAttribute : ValidationAttribute
    {
        private readonly string[] _profanityWords = { "damn", "hell" }; // Mild example words
        
        public override bool IsValid(object value)
        {
            if (value == null || !(value is string title))
                return true; // Let Required handle null validation

            // Check for basic profanity (this is a simple example)
            if (_profanityWords.Any(word => title.Contains(word, StringComparison.OrdinalIgnoreCase)))
                return false;

            // Must not be all numbers
            if (title.All(char.IsDigit))
                return false;

            return true;
        }

        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field contains inappropriate content or invalid format.";
        }
    }
}