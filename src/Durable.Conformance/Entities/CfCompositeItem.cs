namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Entity with a composite primary key (<see cref="TenantId"/>, then <see cref="Sku"/>). Storage: <c>cf_composite_items</c>.
    /// </summary>
    [Entity("cf_composite_items")]
    public class CfCompositeItem
    {
        /// <summary>Gets or sets the second key part.</summary>
        [Property("sku", Flags.PrimaryKey | Flags.String, 32, KeyOrder = 1)]
        public string Sku { get; set; } = string.Empty;

        /// <summary>Gets or sets the first key part.</summary>
        [Property("tenant_id", Flags.PrimaryKey, KeyOrder = 0)]
        public int TenantId { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the quantity.</summary>
        [Property("quantity")]
        public int Quantity { get; set; }
    }
}
