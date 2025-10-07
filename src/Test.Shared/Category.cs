namespace Test.Shared
{
    using Durable;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    
    /// <summary>
    /// Represents a category entity in the database.
    /// </summary>
    [Entity("categories")]
    public class Category
    {
        /// <summary>
        /// Gets or sets the unique identifier for the category.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name of the category.
        /// </summary>
        [Property("name", Flags.String, 100)]
        [Required(ErrorMessage = "Category name is required")]
        [StringLength(100, MinimumLength = 1, ErrorMessage = "Category name must be between 1 and 100 characters")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the description of the category.
        /// </summary>
        [Property("description", Flags.String, 255)]
        [StringLength(255, ErrorMessage = "Description cannot exceed 255 characters")]
        public string Description { get; set; }

        /// <summary>
        /// Gets or sets the list of authors associated with this category.
        /// </summary>
        [ManyToManyNavigationProperty(typeof(AuthorCategory), "CategoryId", "AuthorId")]
        public List<Author> Authors { get; set; } = new List<Author>();

        /// <summary>
        /// Returns a string representation of the category.
        /// </summary>
        /// <returns>A string containing the category's details.</returns>
        public override string ToString()
        {
            return $"Category: Id={Id}, Name={Name}, Description={Description}, Authors Count={Authors?.Count ?? 0}";
        }
    }
}