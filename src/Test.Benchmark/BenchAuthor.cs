namespace Test.Benchmark
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Parent entity for the Include benchmark (100 rows, 10 posts each).
    /// </summary>
    [Entity("bench_authors")]
    public class BenchAuthor
    {
        /// <summary>Primary key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Name.</summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Country.</summary>
        [Property("country", Flags.String, 50)]
        public string? Country { get; set; }

        /// <summary>Posts written by the author.</summary>
        [InverseNavigationProperty("AuthorId")]
        public List<BenchPost> Posts { get; set; } = new List<BenchPost>();
    }
}
