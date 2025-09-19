namespace Test.Shared
{
    using Durable;
    using System.ComponentModel.DataAnnotations;
    
    /// <summary>
    /// Represents a book entity in the database.
    /// </summary>
    [Entity("books")]
    public class Book
    {
        /// <summary>
        /// Gets or sets the unique identifier for the book.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the title of the book.
        /// </summary>
        [Property("title", Flags.String, 200)]
        [Required(ErrorMessage = "Book title is required")]
        [StringLength(200, MinimumLength = 1, ErrorMessage = "Book title must be between 1 and 200 characters")]
        public string Title { get; set; }

        /// <summary>
        /// Gets or sets the ID of the book's author.
        /// </summary>
        [Property("author_id")]
        [ForeignKey(typeof(Author), "Id")]
        [Required(ErrorMessage = "Author ID is required")]
        [Range(1, int.MaxValue, ErrorMessage = "Author ID must be a positive number")]
        public int AuthorId { get; set; }

        /// <summary>
        /// Gets or sets the author navigation property.
        /// </summary>
        [NavigationProperty("AuthorId")]
        public Author Author { get; set; }

        /// <summary>
        /// Gets or sets the ID of the book's publisher (optional).
        /// </summary>
        [Property("publisher_id")]
        [ForeignKey(typeof(Company), "Id")]
        [Range(1, int.MaxValue, ErrorMessage = "Publisher ID must be a positive number when specified")]
        public int? PublisherId { get; set; }

        /// <summary>
        /// Gets or sets the publisher navigation property.
        /// </summary>
        [NavigationProperty("PublisherId")]
        public Company Publisher { get; set; }

        /// <summary>
        /// Returns a string representation of the book.
        /// </summary>
        /// <returns>A string containing the book's details.</returns>
        public override string ToString()
        {
            return $"Book: Id={Id}, Title={Title}, AuthorId={AuthorId}, Author={Author?.Name ?? "null"}, Publisher={Publisher?.Name ?? "null"}";
        }
    }
}