namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity with a two-column composite primary key. The key columns are declared out of key order (Sku first,
    /// KeyOrder = 1; TenantId second, KeyOrder = 0) so that the key is (TenantId, Sku).
    /// </summary>
    [Entity("rel_composite_items")]
    public class RelCompositeItem
    {
        /// <summary>
        /// Gets or sets the SKU; second key column. Never null.
        /// </summary>
        [Property("sku", Flags.PrimaryKey | Flags.String, 64, KeyOrder = 1)]
        public string Sku { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant identifier; first key column.
        /// </summary>
        [Property("tenant_id", Flags.PrimaryKey, KeyOrder = 0)]
        public int TenantId { get; set; }

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
