namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Test entity representing a product.
    /// Used by <see cref="InitializationTests"/>.
    /// </summary>
    [Entity("test_products")]
    public class InitProduct
    {
        /// <summary>
        /// Gets or sets the product ID.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the product name.
        /// </summary>
        [Property("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the UTC creation timestamp.
        /// </summary>
        [Property("created_utc")]
        [DefaultValue(DefaultValueType.CurrentDateTimeUtc)]
        public DateTime CreatedUtc { get; set; }

        /// <summary>
        /// Gets or sets the product GUID.
        /// </summary>
        [Property("guid")]
        [DefaultValue(DefaultValueType.NewGuid)]
        public Guid ProductGuid { get; set; }

        /// <summary>
        /// Gets or sets the product price.
        /// </summary>
        [Property("price")]
        public decimal Price { get; set; }
    }
}
