namespace Test.Shared
{
    /// <summary>
    /// Convention-mapped grouped projection target for <see cref="QtItem"/> used by the query translation suites.
    /// </summary>
    public class QtCategoryTotals
    {
        /// <summary>
        /// Gets or sets the group key. Never null.
        /// </summary>
        public string Category { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the number of rows in the group.
        /// </summary>
        public int ItemCount { get; set; }

        /// <summary>
        /// Gets or sets the number of active rows in the group.
        /// </summary>
        public int ActiveCount { get; set; }

        /// <summary>
        /// Gets or sets the summed price of the group.
        /// </summary>
        public decimal TotalPrice { get; set; }

        /// <summary>
        /// Gets or sets the maximum quantity in the group.
        /// </summary>
        public int MaxQuantity { get; set; }
    }
}
