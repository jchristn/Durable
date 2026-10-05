namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity with a natural (non-generated) string key, used by the Upsert tests.
    /// </summary>
    [Entity("rel_upsert_items")]
    public class RelUpsertItem
    {
        /// <summary>
        /// Gets or sets the natural key. Never null.
        /// </summary>
        [Property("code", Flags.PrimaryKey | Flags.String, 64)]
        public string Code { get; set; } = string.Empty;

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
    }
}
