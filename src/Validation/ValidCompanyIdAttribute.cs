namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;

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
}