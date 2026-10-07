namespace Test.Shared
{
    /// <summary>
    /// Mapping-source test junction between authors and tags, keyed by both columns.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_author_tags")]
    public class MsAuthorTag
    {
        /// <summary>Gets or sets the author key (first key column).</summary>
        [MapColumn("author_ref", Key = true, KeyOrder = 0)]
        [MapForeignKey(typeof(MsAuthor), "Id")]
        public int AuthorId { get; set; }

        /// <summary>Gets or sets the tag key (second key column).</summary>
        [MapColumn("tag_ref", Key = true, KeyOrder = 1)]
        [MapForeignKey(typeof(MsTag), "Id")]
        public int TagId { get; set; }
    }
}
