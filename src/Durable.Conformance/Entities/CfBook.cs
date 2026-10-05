namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Book: reference navigations to <see cref="CfAuthor"/> (required) and <see cref="CfPublisher"/> (optional).
    /// Storage: <c>cf_books</c>.
    /// </summary>
    [Entity("cf_books")]
    public class CfBook
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the title. Never null.</summary>
        [Property("title", Flags.String, 128)]
        public string Title { get; set; } = string.Empty;

        /// <summary>Gets or sets the page count.</summary>
        [Property("pages")]
        public int Pages { get; set; }

        /// <summary>Gets or sets the author key.</summary>
        [Property("author_id")]
        [ForeignKey(typeof(CfAuthor), "Id")]
        public int AuthorId { get; set; }

        /// <summary>Gets or sets the author (loaded by Include); may be null.</summary>
        [NavigationProperty("AuthorId")]
        public CfAuthor? Author { get; set; }

        /// <summary>Gets or sets the optional publisher key; may be null.</summary>
        [Property("publisher_id")]
        [ForeignKey(typeof(CfPublisher), "Id")]
        public int? PublisherId { get; set; }

        /// <summary>Gets or sets the publisher (loaded by Include); may be null.</summary>
        [NavigationProperty("PublisherId")]
        public CfPublisher? Publisher { get; set; }
    }
}
