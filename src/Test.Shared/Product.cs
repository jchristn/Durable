namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Test entity for schema management testing with various index configurations.
    /// Demonstrates single-column indexes, composite indexes, and unique indexes.
    /// </summary>
    [Entity("products")]
    [CompositeIndex("idx_category_price", "category", "price")]
    [CompositeIndex("idx_name_sku", "name", "sku", IsUnique = true)]
    public class Product
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

        #region Public-Members

        /// <summary>
        /// Gets or sets the unique identifier for the product.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the product name with a single-column index.
        /// </summary>
        [Property("name", Flags.String, 200)]
        [Index("idx_product_name")]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the SKU (Stock Keeping Unit) with a unique index.
        /// </summary>
        [Property("sku", Flags.String, 50)]
        [Index("idx_product_sku", isUnique: true)]
        public string Sku { get; set; }

        /// <summary>
        /// Gets or sets the product category (part of composite index).
        /// </summary>
        [Property("category", Flags.String, 100)]
        public string Category { get; set; }

        /// <summary>
        /// Gets or sets the product price (part of composite index).
        /// </summary>
        [Property("price")]
        public decimal Price { get; set; }

        /// <summary>
        /// Gets or sets the stock quantity with an auto-generated index name.
        /// </summary>
        [Property("stock_quantity")]
        [Index]
        public int StockQuantity { get; set; }

        /// <summary>
        /// Gets or sets the description (no index).
        /// </summary>
        [Property("description", Flags.String, 1000)]
        public string? Description { get; set; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="Product"/> class.
        /// </summary>
        public Product()
        {
        }

        #endregion

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    }
}
