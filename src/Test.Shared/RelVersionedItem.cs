namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Auto-increment entity with an integer version column, used by concurrency, upsert and batch-update tests.
    /// </summary>
    [Entity("rel_versioned_items")]
    public class RelVersionedItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the salary.
        /// </summary>
        [Property("salary")]
        public decimal Salary { get; set; }

        /// <summary>
        /// Gets or sets an optional nickname. May be null.
        /// </summary>
        [Property("nickname", Flags.String, 100)]
        public string? Nickname { get; set; }

        /// <summary>
        /// Gets or sets the optimistic concurrency version. Zero means unset (initialized on insert).
        /// </summary>
        [Property("version")]
        [VersionColumn(VersionColumnType.Integer)]
        public int Version { get; set; }
    }
}
