namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Entity used by the BulkInsert tests.
    /// </summary>
    [Entity("rel_bulk_items")]
    public class RelBulkItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the amount.
        /// </summary>
        [Property("amount")]
        public decimal Amount { get; set; }

        /// <summary>
        /// Gets or sets an optional note. May be null.
        /// </summary>
        [Property("note", Flags.String, 100)]
        public string? Note { get; set; }

        /// <summary>
        /// Gets or sets a token generated on insert when empty.
        /// </summary>
        [Property("token")]
        [DefaultValue(DefaultValueType.NewGuid)]
        public Guid Token { get; set; }
    }
}
