namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Represents a many-to-many relationship between authors and categories.
    /// </summary>
    [Entity("author_categories")]
    public class AuthorCategory
    {
        /// <summary>
        /// Gets or sets the unique identifier for the author-category relationship.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the identifier of the author in this relationship.
        /// </summary>
        [Property("author_id")]
        [ForeignKey(typeof(Author), "Id")]
        public int AuthorId { get; set; }

        /// <summary>
        /// Gets or sets the identifier of the category in this relationship.
        /// </summary>
        [Property("category_id")]
        [ForeignKey(typeof(Category), "Id")]
        public int CategoryId { get; set; }

        /// <summary>
        /// Gets or sets the author associated with this relationship.
        /// </summary>
        [NavigationProperty("AuthorId")]
        public Author Author { get; set; }

        /// <summary>
        /// Gets or sets the category associated with this relationship.
        /// </summary>
        [NavigationProperty("CategoryId")]
        public Category Category { get; set; }

        /// <summary>
        /// Returns a string representation of the author-category relationship.
        /// </summary>
        /// <returns>A formatted string containing the relationship details.</returns>
        public override string ToString()
        {
            return $"AuthorCategory: Id={Id}, AuthorId={AuthorId}, CategoryId={CategoryId}";
        }
    }
}