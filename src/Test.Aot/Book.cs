namespace Test.Aot
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Book: reference navigation to <see cref="Author"/>, collection navigation to <see cref="Review"/>, optimistic
    /// concurrency, a value converter and JSON columns.
    /// </summary>
    [Entity("books")]
    public class Book
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("title", Flags.String, 200)]
        public string Title { get; set; } = string.Empty;

        [Property("author_id")]
        [ForeignKey(typeof(Author), "Id")]
        public int AuthorId { get; set; }

        [Property("price")]
        public decimal Price { get; set; }

        [Property("published")]
        public DateTime? Published { get; set; }

        [Property("isbn", Flags.String, 32)]
        [ValueConverter(typeof(IsbnConverter))]
        public Isbn? Isbn { get; set; }

        [Property("tags", Flags.Json)]
        public List<string> Tags { get; set; } = new List<string>();

        [Property("details", Flags.Json)]
        public BookDetails? Details { get; set; }

        [Property("version")]
        [VersionColumn(VersionColumnType.Integer)]
        public int Version { get; set; }

        [NavigationProperty("AuthorId")]
        public Author? Author { get; set; }

        [InverseNavigationProperty("BookId")]
        public List<Review> Reviews { get; set; } = new List<Review>();
    }
}
