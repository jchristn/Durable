namespace Test.Shared.CliFixtures.Entities
{
    using System;
    using Durable;

    /// <summary>
    /// CLI fixture entity used by the schema diff/sync tests.
    /// </summary>
    [Entity("cli_orders")]
    public class CliOrder
    {
        /// <summary>Gets or sets the key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the customer key.</summary>
        [Property("customer_id")]
        public int CustomerId { get; set; }

        /// <summary>Gets or sets the order total.</summary>
        [Property("total")]
        public decimal Total { get; set; }

        /// <summary>Gets or sets when the order was created.</summary>
        [Property("created_utc")]
        public DateTime CreatedUtc { get; set; }
    }
}
