namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;
    using Test.Shared;

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

    /// <summary>
    /// Validates that a person's age is reasonable for their department
    /// </summary>
    public class ReasonableAgeForDepartmentAttribute : ValidationAttribute
    {
        public string DepartmentProperty { get; set; }

        public ReasonableAgeForDepartmentAttribute(string departmentProperty = "Department")
        {
            DepartmentProperty = departmentProperty;
        }

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (value == null || !(value is int age))
                return ValidationResult.Success;

            var departmentProperty = validationContext.ObjectType.GetProperty(DepartmentProperty);
            if (departmentProperty == null)
                return ValidationResult.Success;

            var department = departmentProperty.GetValue(validationContext.ObjectInstance) as string;
            if (string.IsNullOrEmpty(department))
                return ValidationResult.Success;

            // Business rules for age validation by department
            switch (department.ToLowerInvariant())
            {
                case "engineering":
                case "sales":
                    if (age < 18 || age > 70)
                        return new ValidationResult($"Age for {department} department must be between 18 and 70.");
                    break;
                case "hr":
                    if (age < 25 || age > 65)
                        return new ValidationResult($"Age for {department} department must be between 25 and 65 (requires experience).");
                    break;
                case "marketing":
                    if (age < 20 || age > 60)
                        return new ValidationResult($"Age for {department} department must be between 20 and 60.");
                    break;
            }

            return ValidationResult.Success;
        }
    }

    /// <summary>
    /// Validates that salary is reasonable for the department and age
    /// </summary>
    public class ReasonableSalaryAttribute : ValidationAttribute
    {
        public string DepartmentProperty { get; set; }
        public string AgeProperty { get; set; }

        public ReasonableSalaryAttribute(string departmentProperty = "Department", string ageProperty = "Age")
        {
            DepartmentProperty = departmentProperty;
            AgeProperty = ageProperty;
        }

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if (value == null || !(value is decimal salary))
                return ValidationResult.Success;

            var departmentProperty = validationContext.ObjectType.GetProperty(DepartmentProperty);
            var ageProperty = validationContext.ObjectType.GetProperty(AgeProperty);

            if (departmentProperty == null || ageProperty == null)
                return ValidationResult.Success;

            var department = departmentProperty.GetValue(validationContext.ObjectInstance) as string;
            var age = ageProperty.GetValue(validationContext.ObjectInstance);

            if (string.IsNullOrEmpty(department) || !(age is int personAge))
                return ValidationResult.Success;

            // Business rules for salary validation
            decimal minSalary = department.ToLowerInvariant() switch
            {
                "engineering" => 50000,
                "sales" => 40000,
                "hr" => 45000,
                "marketing" => 35000,
                _ => 30000
            };

            decimal maxSalary = department.ToLowerInvariant() switch
            {
                "engineering" => 200000,
                "sales" => 150000,
                "hr" => 120000,
                "marketing" => 100000,
                _ => 80000
            };

            // Adjust for experience (age-based)
            if (personAge > 40)
            {
                maxSalary *= 1.5m; // Senior employees can earn more
            }
            else if (personAge < 25)
            {
                minSalary *= 0.8m; // Junior employees might earn less
            }

            if (salary < minSalary)
                return new ValidationResult($"Salary is too low for {department} department. Minimum: ${minSalary:N0}");

            if (salary > maxSalary)
                return new ValidationResult($"Salary is too high for {department} department and experience level. Maximum: ${maxSalary:N0}");

            return ValidationResult.Success;
        }
    }
}