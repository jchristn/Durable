namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Entity with an integer version column for optimistic concurrency. Storage: <c>cf_versioned_items</c>.
    /// </summary>
    [Entity("cf_versioned_items")]
    public class CfVersionedItem
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the salary.</summary>
        [Property("salary")]
        public decimal Salary { get; set; }

        /// <summary>Gets or sets an optional nickname; may be null.</summary>
        [Property("nickname", Flags.String, 64)]
        public string? Nickname { get; set; }

        /// <summary>Gets or sets the version (1 after create, incremented by every update).</summary>
        [Property("version")]
        [VersionColumn(VersionColumnType.Integer)]
        public int Version { get; set; }
    }
}
