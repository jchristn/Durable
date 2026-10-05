namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Read model over <c>qt_items</c> that additionally maps the aliased columns produced by window functions and
    /// CASE expressions in the query translation suites. Never used to create or write the table.
    /// </summary>
    [Entity("qt_items")]
    public class QtItemComputedRow
    {
        /// <summary>
        /// Gets or sets the primary key.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the item name. Never null.
        /// </summary>
        [Property("name", Flags.String, 128)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the price.
        /// </summary>
        [Property("price")]
        public decimal Price { get; set; }

        /// <summary>
        /// Gets or sets the category. Never null.
        /// </summary>
        [Property("category", Flags.String, 32)]
        public string Category { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the ROW_NUMBER() alias <c>rn</c>. Null when not selected.
        /// </summary>
        [Property("rn")]
        public long? RowNumber { get; set; }

        /// <summary>
        /// Gets or sets the RANK() alias <c>rnk</c>. Null when not selected.
        /// </summary>
        [Property("rnk")]
        public long? RankValue { get; set; }

        /// <summary>
        /// Gets or sets the default-named ROW_NUMBER() alias <c>row_number</c>. Null when not selected.
        /// </summary>
        [Property("row_number")]
        public long? DefaultRowNumber { get; set; }

        /// <summary>
        /// Gets or sets the windowed COUNT(*) alias <c>cnt</c>. Null when not selected.
        /// </summary>
        [Property("cnt")]
        public long? PartitionCount { get; set; }

        /// <summary>
        /// Gets or sets the running SUM() alias <c>running_total</c>. Null when not selected.
        /// </summary>
        [Property("running_total")]
        public decimal? RunningTotal { get; set; }

        /// <summary>
        /// Gets or sets the LEAD() alias <c>next_name</c>. Null when not selected.
        /// </summary>
        [Property("next_name")]
        public string? NextName { get; set; }

        /// <summary>
        /// Gets or sets the LAG() alias <c>prev_price</c>. Null when not selected.
        /// </summary>
        [Property("prev_price")]
        public decimal? PreviousPrice { get; set; }

        /// <summary>
        /// Gets or sets the CASE alias <c>tier</c>. Null when not selected.
        /// </summary>
        [Property("tier")]
        public string? Tier { get; set; }
    }
}
