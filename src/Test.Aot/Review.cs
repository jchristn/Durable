namespace Test.Aot
{
    using Durable;

    /// <summary>
    /// Review of a book (reached by ThenInclude).
    /// </summary>
    [Entity("reviews")]
    public class Review
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("book_id")]
        [ForeignKey(typeof(Book), "Id")]
        public int BookId { get; set; }

        [Property("stars")]
        public int Stars { get; set; }

        [Property("text", Flags.String, 500)]
        public string? Text { get; set; }
    }
}
