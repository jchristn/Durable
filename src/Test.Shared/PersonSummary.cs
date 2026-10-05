namespace Test.Shared
{
    /// <summary>
    /// Projection result type used by projection/select test suites.
    /// </summary>
    public class PersonSummary
    {
        /// <summary>Gets or sets the first name.</summary>
        public string FirstName { get; set; } = string.Empty;

        /// <summary>Gets or sets the last name.</summary>
        public string LastName { get; set; } = string.Empty;

        /// <summary>Gets or sets the email. Null when the source column is null.</summary>
        public string? Email { get; set; } = string.Empty;

        /// <summary>Gets or sets the salary.</summary>
        public decimal Salary { get; set; }
    }
}
