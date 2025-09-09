namespace TestApi.Validation
{
    using System.ComponentModel.DataAnnotations;

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
}