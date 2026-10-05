namespace Test.Benchmark
{
    using System;
    using Durable;

    /// <summary>
    /// Child entity for the Include benchmark.
    /// </summary>
    [Entity("bench_posts")]
    public class BenchPost
    {
        /// <summary>Primary key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Owning author.</summary>
        [Property("author_id")]
        [ForeignKey(typeof(BenchAuthor), "Id")]
        public int AuthorId { get; set; }

        /// <summary>Title.</summary>
        [Property("title", Flags.String, 200)]
        public string Title { get; set; } = string.Empty;

        /// <summary>Publication time.</summary>
        [Property("published_utc")]
        public DateTime PublishedUtc { get; set; }

        /// <summary>View count.</summary>
        [Property("views")]
        public int Views { get; set; }
    }
}
