namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;

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