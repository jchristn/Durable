namespace Durable.Conformance
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Author: reference navigation to <see cref="CfPublisher"/>, collection navigations to <see cref="CfBook"/> and
    /// soft-deletable <see cref="CfNote"/> rows, and a many-to-many navigation to <see cref="CfTag"/> through
    /// <see cref="CfAuthorTag"/>. Storage: <c>cf_authors</c>.
    /// </summary>
    [Entity("cf_authors")]
    public class CfAuthor
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the optional publisher key; may be null.</summary>
        [Property("publisher_id")]
        [ForeignKey(typeof(CfPublisher), "Id")]
        public int? PublisherId { get; set; }

        /// <summary>Gets or sets the publisher (loaded by Include); may be null.</summary>
        [NavigationProperty("PublisherId")]
        public CfPublisher? Publisher { get; set; }

        /// <summary>Gets or sets the books (loaded by Include). Never null.</summary>
        [InverseNavigationProperty("AuthorId")]
        public List<CfBook> Books { get; set; } = new List<CfBook>();

        /// <summary>Gets or sets the notes (loaded by Include; soft-deleted notes excluded). Never null.</summary>
        [InverseNavigationProperty("AuthorId")]
        public List<CfNote> Notes { get; set; } = new List<CfNote>();

        /// <summary>Gets or sets the tags (many-to-many, loaded by Include). Never null.</summary>
        [ManyToManyNavigationProperty(typeof(CfAuthorTag), "AuthorId", "TagId")]
        public List<CfTag> Tags { get; set; } = new List<CfTag>();
    }
}
