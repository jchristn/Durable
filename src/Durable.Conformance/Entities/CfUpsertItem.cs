namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Entity with a natural (caller-assigned) string key, used by upsert cases. Storage: <c>cf_upsert_items</c>.
    /// </summary>
    [Entity("cf_upsert_items")]
    public class CfUpsertItem
    {
        /// <summary>Gets or sets the natural key. Never null.</summary>
        [Property("code", Flags.PrimaryKey | Flags.String, 32)]
        public string Code { get; set; } = string.Empty;

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the quantity.</summary>
        [Property("quantity")]
        public int Quantity { get; set; }
    }
}
