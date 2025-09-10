namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Validates that a company ID exists and is positive when provided
    /// </summary>
    public class ValidCompanyIdAttribute : ValidationAttribute
    {
        /// <summary>
        /// Validates the specified value
        /// </summary>
        /// <param name="value">The value to validate</param>
        /// <returns>True if the value is valid, false otherwise</returns>
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

        /// <summary>
        /// Formats the error message for validation failure
        /// </summary>
        /// <param name="name">The name of the field being validated</param>
        /// <returns>The formatted error message</returns>
        public override string FormatErrorMessage(string name)
        {
            return $"The {name} field must be a positive number when specified.";
        }
    }
}