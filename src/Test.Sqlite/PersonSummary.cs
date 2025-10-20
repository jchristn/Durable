namespace Test.Sqlite
{
    /// <summary>
    /// Represents a summary of person information for testing purposes.
    /// </summary>
    public class PersonSummary
    {
        /// <summary>
        /// Gets or sets the first name.
        /// </summary>
        public string FirstName { get; set; }

        /// <summary>
        /// Gets or sets the last name.
        /// </summary>
        public string LastName { get; set; }

        /// <summary>
        /// Gets or sets the email address.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        /// Gets or sets the salary.
        /// </summary>
        public decimal Salary { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PersonSummary"/> class.
        /// </summary>
        public PersonSummary()
        {
        }
    }
}