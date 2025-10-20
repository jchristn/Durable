namespace Test.Sqlite
{
    using System;
    using Durable;

    /// <summary>
    /// Represents a test person entity for testing purposes.
    /// </summary>
    [Entity("TestPerson")]
    public class TestPerson
    {
        /// <summary>
        /// Gets or sets the unique identifier.
        /// </summary>
        [Property("Id", Flags.PrimaryKey)]
        public int Id { get; set; }
        
        [Property("FirstName", Flags.String, 64)]
        public string FirstName { get; set; }
        
        [Property("LastName", Flags.String, 64)]
        public string LastName { get; set; }
        
        [Property("Salary")]
        public decimal Salary { get; set; }
        
        [Property("Age")]
        public int Age { get; set; }
        
        [Property("LastModified")]
        public DateTime LastModified { get; set; }
        
        [Property("Department", Flags.String, 32)]
        public string Department { get; set; }
        
        [Property("Bonus")]
        public decimal Bonus { get; set; }
        
        [Property("Status", Flags.String, 16)]
        public string Status { get; set; }
        
        [Property("YearsOfService")]
        public int YearsOfService { get; set; }
        
        /// <summary>
        /// Gets or sets the email address.
        /// </summary>
        [Property("Email", Flags.String, 128)]
        public string Email { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TestPerson"/> class.
        /// </summary>
        public TestPerson()
        {
        }
    }
}