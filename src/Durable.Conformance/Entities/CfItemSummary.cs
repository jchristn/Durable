namespace Durable.Conformance
{
    /// <summary>
    /// Projection target for <see cref="CfItem"/> (not an entity).
    /// </summary>
    public class CfItemSummary
    {
        /// <summary>Gets or sets the name.</summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the category.</summary>
        public string Category { get; set; } = string.Empty;

        /// <summary>Gets or sets price times quantity.</summary>
        public decimal Total { get; set; }

        /// <summary>Gets or sets whether the item is active.</summary>
        public bool IsActive { get; set; }

        /// <summary>Gets or sets whether the price exceeds 50.</summary>
        public bool IsExpensive { get; set; }

        /// <summary>Gets or sets "premium" or "standard".</summary>
        public string Tier { get; set; } = string.Empty;

        /// <summary>Gets or sets the status.</summary>
        public CfStatus Status { get; set; }

        /// <summary>Gets or sets the priority.</summary>
        public CfPriority Priority { get; set; }

        /// <summary>Gets or sets "Name (Category)".</summary>
        public string Label { get; set; } = string.Empty;

        /// <summary>Gets or sets the name length (computed with a function).</summary>
        public int NameLength { get; set; }

        /// <summary>Gets or sets the discount; may be null.</summary>
        public int? Discount { get; set; }
    }
}
