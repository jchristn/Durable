namespace Durable.Conformance
{
    /// <summary>
    /// Grouped projection target: per-category aggregates of <see cref="CfItem"/> (not an entity).
    /// </summary>
    public class CfCategoryTotals
    {
        /// <summary>Gets or sets the category (group key).</summary>
        public string Category { get; set; } = string.Empty;

        /// <summary>Gets or sets the number of items.</summary>
        public int ItemCount { get; set; }

        /// <summary>Gets or sets the number of active items.</summary>
        public int ActiveCount { get; set; }

        /// <summary>Gets or sets the price total.</summary>
        public decimal TotalPrice { get; set; }

        /// <summary>Gets or sets the largest quantity.</summary>
        public int MaxQuantity { get; set; }

        /// <summary>Gets or sets the smallest quantity.</summary>
        public int MinQuantity { get; set; }

        /// <summary>Gets or sets the average price.</summary>
        public decimal AveragePrice { get; set; }
    }
}
