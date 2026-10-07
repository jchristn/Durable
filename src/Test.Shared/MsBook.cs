namespace Test.Shared
{
    /// <summary>
    /// Mapping-source test book with a reference navigation and a converted price.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_books")]
    public class MsBook
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("book_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the title.</summary>
        [MapColumn("title", Length = 128)]
        public string Title { get; set; } = string.Empty;

        /// <summary>Gets or sets the author key.</summary>
        [MapColumn("author_ref")]
        [MapForeignKey(typeof(MsAuthor), "Id")]
        public int AuthorId { get; set; }

        /// <summary>Gets or sets the price, stored as cents.</summary>
        [MapColumn("price_cents")]
        [MapConverter(typeof(MoneyConverter))]
        public Money Price { get; set; }

        /// <summary>Gets or sets the author.</summary>
        [MapReference("AuthorId")]
        public MsAuthor? Author { get; set; }
    }
}
