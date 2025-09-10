namespace Test.Shared
{
    using Durable;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    
    /// <summary>
    /// Represents a company entity with employees and published books.
    /// </summary>
    [Entity("companies")]
    public class Company
    {
        /// <summary>
        /// Gets or sets the unique identifier for the company.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name of the company.
        /// </summary>
        [Property("name", Flags.String, 100)]
        [Required(ErrorMessage = "Company name is required")]
        [StringLength(100, MinimumLength = 1, ErrorMessage = "Company name must be between 1 and 100 characters")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the industry that the company operates in.
        /// </summary>
        [Property("industry", Flags.String, 50)]
        [StringLength(50, ErrorMessage = "Industry name cannot exceed 50 characters")]
        public string Industry { get; set; }

        /// <summary>
        /// Gets or sets the list of authors who are employees of this company.
        /// </summary>
        [InverseNavigationProperty("CompanyId")]
        public List<Author> Employees { get; set; } = new List<Author>();

        /// <summary>
        /// Gets or sets the list of books published by this company.
        /// </summary>
        [InverseNavigationProperty("PublisherId")]
        public List<Book> PublishedBooks { get; set; } = new List<Book>();

        /// <summary>
        /// Returns a string representation of the company.
        /// </summary>
        /// <returns>A string containing the company's details.</returns>
        public override string ToString()
        {
            return $"Company: Id={Id}, Name={Name}, Industry={Industry}, Employees={Employees?.Count ?? 0}, Published Books={PublishedBooks?.Count ?? 0}";
        }
    }
}