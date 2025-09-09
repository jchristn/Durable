namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Validates that a book title is appropriate and doesn't contain profanity
    /// </summary>
    public class ValidBookTitleAttribute : ValidationAttribute
    {
        private readonly string[] ProfanityWords = { "damn", "hell" }; // Mild example words
        
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

        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field contains inappropriate content or invalid format.";
        }
    }
}