namespace Test.Aot
{
    using Durable;

    /// <summary>
    /// Junction between <see cref="Author"/> and <see cref="Category"/>.
    /// </summary>
    [Entity("author_categories")]
    public class AuthorCategory
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("author_id")]
        [ForeignKey(typeof(Author), "Id")]
        public int AuthorId { get; set; }

        [Property("category_id")]
        [ForeignKey(typeof(Category), "Id")]
        public int CategoryId { get; set; }
    }
}
