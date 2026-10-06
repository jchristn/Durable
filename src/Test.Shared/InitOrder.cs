namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Test entity representing an order.
    /// Used by <see cref="InitializationTests"/>.
    /// </summary>
    [Entity("test_orders")]
    public class InitOrder
    {
        /// <summary>
        /// Gets or sets the order ID.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the product ID.
        /// </summary>
        [Property("product_id")]
        [ForeignKey(typeof(InitProduct), nameof(InitProduct.Id))]
        public int ProductId { get; set; }

        /// <summary>
        /// Gets or sets the order quantity.
        /// </summary>
        [Property("quantity")]
        public int Quantity { get; set; }

        /// <summary>
        /// Gets or sets the order date.
        /// </summary>
        [Property("order_date")]
        [DefaultValue(DefaultValueType.CurrentDateTimeUtc)]
        public DateTime OrderDate { get; set; }
    }
}
