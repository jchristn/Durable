namespace Test.Aot
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Author: one-to-many to <see cref="Book"/>, many-to-many to <see cref="Category"/>, enum, date, nullable and soft delete columns.
    /// </summary>
    [Entity("authors")]
    public class Author
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        [Property("email", Flags.String, 200)]
        public string? Email { get; set; }

        [Property("rating")]
        public int Rating { get; set; }

        [Property("status")]
        public AuthorStatus Status { get; set; } = AuthorStatus.Active;

        [Property("born")]
        public DateTime Born { get; set; }

        [Property("score")]
        public decimal? Score { get; set; }

        [Property("deleted")]
        [SoftDelete]
        public bool Deleted { get; set; }

        [InverseNavigationProperty("AuthorId")]
        public List<Book> Books { get; set; } = new List<Book>();

        [ManyToManyNavigationProperty(typeof(AuthorCategory), "AuthorId", "CategoryId")]
        public List<Category> Categories { get; set; } = new List<Category>();
    }
}
