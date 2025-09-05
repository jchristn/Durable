namespace Test.Shared
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Durable;

    // Person model
    [Entity("people")]
    public class Person
    {
        #region Public-Members

        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("first", Flags.String, 64)]
        [Required(ErrorMessage = "First name is required")]
        [StringLength(64, MinimumLength = 1, ErrorMessage = "First name must be between 1 and 64 characters")]
        public string FirstName { get; set; }

        [Property("last", Flags.String, 64)]
        [Required(ErrorMessage = "Last name is required")]
        [StringLength(64, MinimumLength = 1, ErrorMessage = "Last name must be between 1 and 64 characters")]
        public string LastName { get; set; }

        [Property("age")]
        [Required(ErrorMessage = "Age is required")]
        [Range(0, 150, ErrorMessage = "Age must be between 0 and 150")]
        public int Age { get; set; }

        [Property("email", Flags.String, 128)]
        [EmailAddress(ErrorMessage = "Please provide a valid email address")]
        [StringLength(128, ErrorMessage = "Email cannot exceed 128 characters")]
        public string Email { get; set; }

        [Property("salary")]
        [Required(ErrorMessage = "Salary is required")]
        [Range(0, double.MaxValue, ErrorMessage = "Salary must be non-negative")]
        public decimal Salary { get; set; }

        [Property("department", Flags.String, 32)]
        [StringLength(32, ErrorMessage = "Department name cannot exceed 32 characters")]
        public string Department { get; set; }

        public string Name => $"{FirstName} {LastName}";

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        public override string ToString()
        {
            return $"Person: Id={Id}, Name={FirstName} {LastName}, Age={Age}, Email={Email}, Salary={Salary:C}, Dept={Department}";
        }

        #endregion

        #region Private-Methods

        #endregion
    }

}
