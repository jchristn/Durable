namespace Test.Shared
{
    using System.Collections.Generic;

    /// <summary>
    /// Mapping-source test author with books (one-to-many) and tags (many-to-many).
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_authors")]
    public class MsAuthor
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("author_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        [MapColumn("author_name", Length = 64)]
        [MapIndex("idx_ms_authors_name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets a value that is not mapped (no MapColumn on an attribute-mapped class).</summary>
        public string Scratch { get; set; } = string.Empty;

        /// <summary>Gets or sets the books.</summary>
        [MapCollection("AuthorId")]
        public List<MsBook> Books { get; set; } = new List<MsBook>();

        /// <summary>Gets or sets the tags.</summary>
        [MapManyToMany(typeof(MsAuthorTag), "AuthorId", "TagId")]
        public List<MsTag> Tags { get; set; } = new List<MsTag>();
    }
}
