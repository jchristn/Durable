namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Auto-increment entity whose version column is a <c>byte[]</c> with an inferred
    /// <see cref="VersionColumnType.BinaryCounter"/> type (the attribute declares no type).
    /// </summary>
    [Entity("rel_binary_versioned_items")]
    public class RelBinaryVersionedItem
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
        /// Gets or sets the quantity.
        /// </summary>
        [Property("quantity")]
        public int Quantity { get; set; }

        /// <summary>
        /// Gets or sets the 8-byte version counter. Null or empty means unset (initialized on insert).
        /// </summary>
        [Property("row_version", Flags.None, 8)]
        [VersionColumn]
        public byte[]? RowVersion { get; set; }
    }
}
