namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// DuckDB schema-sync entity: the slim version of duck_indexed (two indexed columns).
    /// </summary>
    [Entity("duck_indexed")]
    public class DuckIndexedSlim
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the indexed name. May be null.
        /// </summary>
        [Property("name", Flags.String, 50)]
        [Index("idx_duck_indexed_name")]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the indexed code. May be null.
        /// </summary>
        [Property("code", Flags.String, 20)]
        [Index("idx_duck_indexed_code")]
        public string? Code { get; set; }
    }
}
