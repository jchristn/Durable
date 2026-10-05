namespace Test.Shared
{
    /// <summary>
    /// Convention-mapped projection target for <see cref="QtItem"/> used by the query translation suites.
    /// </summary>
    public class QtItemSummary
    {
        /// <summary>
        /// Gets or sets the item name. Never null.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the category. Never null.
        /// </summary>
        public string Category { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the computed total (price times quantity).
        /// </summary>
        public decimal Total { get; set; }

        /// <summary>
        /// Gets or sets the active flag.
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// Gets or sets a computed boolean.
        /// </summary>
        public bool IsExpensive { get; set; }

        /// <summary>
        /// Gets or sets a computed tier label. Never null.
        /// </summary>
        public string Tier { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the computed name length.
        /// </summary>
        public int NameLength { get; set; }

        /// <summary>
        /// Gets or sets the status.
        /// </summary>
        public QtStatus Status { get; set; }

        /// <summary>
        /// Gets or sets the priority.
        /// </summary>
        public QtPriority Priority { get; set; }

        /// <summary>
        /// Gets or sets a computed label (string concatenation). Never null.
        /// </summary>
        public string Label { get; set; } = string.Empty;
    }
}
