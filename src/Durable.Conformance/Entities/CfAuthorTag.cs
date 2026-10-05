namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Junction entity linking <see cref="CfAuthor"/> and <see cref="CfTag"/>. Storage: <c>cf_author_tags</c>.
    /// </summary>
    [Entity("cf_author_tags")]
    public class CfAuthorTag
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the author key.</summary>
        [Property("author_id")]
        [ForeignKey(typeof(CfAuthor), "Id")]
        public int AuthorId { get; set; }

        /// <summary>Gets or sets the tag key.</summary>
        [Property("tag_id")]
        [ForeignKey(typeof(CfTag), "Id")]
        public int TagId { get; set; }
    }
}
