namespace Test.Shared
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Durable;

    /// <summary>
    /// Represents a person entity with personal and professional information.
    /// </summary>
    [Entity("people")]
    public class Person
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the unique identifier for the person.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the first name of the person.
        /// </summary>
        [Property("first", Flags.String, 64)]
        [Required(ErrorMessage = "First name is required")]
        [StringLength(64, MinimumLength = 1, ErrorMessage = "First name must be between 1 and 64 characters")]
        public string FirstName { get; set; }

        /// <summary>
        /// Gets or sets the last name of the person.
        /// </summary>
        [Property("last", Flags.String, 64)]
        [Required(ErrorMessage = "Last name is required")]
        [StringLength(64, MinimumLength = 1, ErrorMessage = "Last name must be between 1 and 64 characters")]
        public string LastName { get; set; }

        /// <summary>
        /// Gets or sets the age of the person in years.
        /// </summary>
        [Property("age")]
        [Required(ErrorMessage = "Age is required")]
        [Range(0, 150, ErrorMessage = "Age must be between 0 and 150")]
        public int Age { get; set; }

        /// <summary>
        /// Gets or sets the email address of the person.
        /// </summary>
        [Property("email", Flags.String, 128)]
        [EmailAddress(ErrorMessage = "Please provide a valid email address")]
        [StringLength(128, ErrorMessage = "Email cannot exceed 128 characters")]
        public string Email { get; set; }

        /// <summary>
        /// Gets or sets the salary of the person.
        /// </summary>
        [Property("salary")]
        [Required(ErrorMessage = "Salary is required")]
        [Range(0, double.MaxValue, ErrorMessage = "Salary must be non-negative")]
        public decimal Salary { get; set; }

        /// <summary>
        /// Gets or sets the department where the person works.
        /// </summary>
        [Property("department", Flags.String, 32)]
        [StringLength(32, ErrorMessage = "Department name cannot exceed 32 characters")]
        public string Department { get; set; }

        /// <summary>
        /// Gets the full name of the person by combining first and last name.
        /// </summary>
        public string Name => $"{FirstName} {LastName}";

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string representation of the person including key information.
        /// </summary>
        /// <returns>A formatted string containing the person's details.</returns>
        public override string ToString()
        {
            return $"Person: Id={Id}, Name={FirstName} {LastName}, Age={Age}, Email={Email}, Salary={Salary:C}, Dept={Department}";
        }

        #endregion

        #region Private-Methods

        #endregion
    }

}
