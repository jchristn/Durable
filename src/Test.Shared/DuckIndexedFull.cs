namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// DuckDB schema-sync entity: the full version of duck_indexed, adding a nullable column and a NOT NULL column to
    /// <see cref="DuckIndexedSlim"/> (DuckDB cannot drop either, or make the second NOT NULL, while the table has indexes).
    /// </summary>
    [Entity("duck_indexed")]
    public class DuckIndexedFull
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

        /// <summary>
        /// Gets or sets a nullable column only the full version maps. May be null.
        /// </summary>
        [Property("legacy", Flags.String, 50)]
        public string? Legacy { get; set; }

        /// <summary>
        /// Gets or sets a NOT NULL column only the full version maps (added with DEFAULT 0).
        /// </summary>
        [Property("qty")]
        public int Qty { get; set; }
    }
}
