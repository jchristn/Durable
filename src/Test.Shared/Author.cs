namespace Test.Shared
{
    using Durable;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    
    /// <summary>
    /// Represents an author entity with associated company, books, and categories.
    /// </summary>
    [Entity("authors")]
    public class Author
    {
        /// <summary>
        /// Gets or sets the unique identifier for the author.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the author's name. Must be between 1 and 100 characters.
        /// </summary>
        [Property("name", Flags.String, 100)]
        [Required(ErrorMessage = "Author name is required")]
        [StringLength(100, MinimumLength = 1, ErrorMessage = "Author name must be between 1 and 100 characters")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the optional company identifier that the author is associated with.
        /// </summary>
        [Property("company_id")]
        [ForeignKey(typeof(Company), "Id")]
        [Range(1, int.MaxValue, ErrorMessage = "Company ID must be a positive number when specified")]
        public int? CompanyId { get; set; }

        /// <summary>
        /// Gets or sets the company that the author is associated with.
        /// </summary>
        [NavigationProperty("CompanyId")]
        public Company Company { get; set; }

        /// <summary>
        /// Gets or sets the collection of books written by this author.
        /// </summary>
        [InverseNavigationProperty("AuthorId")]
        public List<Book> Books { get; set; } = new List<Book>();

        /// <summary>
        /// Gets or sets the collection of categories that this author is associated with.
        /// </summary>
        [ManyToManyNavigationProperty(typeof(AuthorCategory), "AuthorId", "CategoryId")]
        public List<Category> Categories { get; set; } = new List<Category>();

        /// <summary>
        /// Returns a string representation of the author including ID, name, company information, and counts of books and categories.
        /// </summary>
        /// <returns>A formatted string containing author details.</returns>
        public override string ToString()
        {
            return $"Author: Id={Id}, Name={Name}, CompanyId={CompanyId}, Company={Company?.Name ?? "null"}, Books Count={Books?.Count ?? 0}, Categories Count={Categories?.Count ?? 0}";
        }
    }
}