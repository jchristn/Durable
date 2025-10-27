namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Test entity for schema management testing with composite indexes using IndexAttribute.
    /// Demonstrates creating composite indexes by using the same index name on multiple properties.
    /// </summary>
    [Entity("employees")]
    public class Employee
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

        #region Public-Members

        /// <summary>
        /// Gets or sets the unique identifier for the employee.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the first name (part of composite index on name).
        /// </summary>
        [Property("first_name", Flags.String, 100)]
        [Index("idx_full_name", Order = 0)]
        public string FirstName { get; set; }

        /// <summary>
        /// Gets or sets the last name (part of composite index on name).
        /// </summary>
        [Property("last_name", Flags.String, 100)]
        [Index("idx_full_name", Order = 1)]
        public string LastName { get; set; }

        /// <summary>
        /// Gets or sets the email with a unique index.
        /// </summary>
        [Property("email", Flags.String, 255)]
        [Index("idx_employee_email", isUnique: true)]
        public string Email { get; set; }

        /// <summary>
        /// Gets or sets the department (part of composite index on department and hire date).
        /// </summary>
        [Property("department", Flags.String, 100)]
        [Index("idx_dept_hire_date", Order = 0)]
        public string Department { get; set; }

        /// <summary>
        /// Gets or sets the hire date (part of composite index on department and hire date).
        /// </summary>
        [Property("hire_date")]
        [Index("idx_dept_hire_date", Order = 1)]
        public System.DateTime HireDate { get; set; }

        /// <summary>
        /// Gets or sets the salary (no index).
        /// </summary>
        [Property("salary")]
        public decimal Salary { get; set; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="Employee"/> class.
        /// </summary>
        public Employee()
        {
        }

        #endregion

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    }
}
