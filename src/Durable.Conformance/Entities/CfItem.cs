namespace Durable.Conformance
{
    using System;
    using Durable;

    /// <summary>
    /// The main scalar entity: identity key, strings, numbers, booleans, dates, a Guid, enums stored by name and as integers,
    /// and a nullable reference navigation to <see cref="CfOwner"/>. Storage: <c>cf_items</c>.
    /// </summary>
    [Entity("cf_items")]
    public class CfItem
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 128)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets a code that may contain special characters; may be null.</summary>
        [Property("code", Flags.String, 128)]
        public string? Code { get; set; }

        /// <summary>Gets or sets an email; may be null, empty or whitespace.</summary>
        [Property("email", Flags.String, 128)]
        public string? Email { get; set; }

        /// <summary>Gets or sets the status (stored by name).</summary>
        [Property("status", Flags.String, 32)]
        public CfStatus Status { get; set; }

        /// <summary>Gets or sets the priority (stored as an integer).</summary>
        [Property("priority", Flags.Integer)]
        public CfPriority Priority { get; set; }

        /// <summary>Gets or sets the price.</summary>
        [Property("price")]
        public decimal Price { get; set; }

        /// <summary>Gets or sets a double value.</summary>
        [Property("ratio")]
        public double Ratio { get; set; }

        /// <summary>Gets or sets the quantity.</summary>
        [Property("quantity")]
        public int Quantity { get; set; }

        /// <summary>Gets or sets an optional discount; may be null.</summary>
        [Property("discount")]
        public int? Discount { get; set; }

        /// <summary>Gets or sets a 64-bit value.</summary>
        [Property("big_number")]
        public long BigNumber { get; set; }

        /// <summary>Gets or sets whether the item is active.</summary>
        [Property("is_active")]
        public bool IsActive { get; set; }

        /// <summary>Gets or sets an optional flag; may be null.</summary>
        [Property("is_featured")]
        public bool? IsFeatured { get; set; }

        /// <summary>Gets or sets the creation time (whole seconds, unspecified kind).</summary>
        [Property("created_utc")]
        public DateTime CreatedUtc { get; set; }

        /// <summary>Gets or sets an optional due date; may be null.</summary>
        [Property("due_date")]
        public DateTime? DueDate { get; set; }

        /// <summary>Gets or sets a Guid.</summary>
        [Property("token")]
        public Guid Token { get; set; }

        /// <summary>Gets or sets the category. Never null.</summary>
        [Property("category", Flags.String, 32)]
        public string Category { get; set; } = string.Empty;

        /// <summary>Gets or sets the optional owner key; may be null.</summary>
        [Property("owner_id")]
        [ForeignKey(typeof(CfOwner), "Id")]
        public int? OwnerId { get; set; }

        /// <summary>Gets or sets the owner (loaded by Include); may be null.</summary>
        [NavigationProperty("OwnerId")]
        public CfOwner? Owner { get; set; }
    }
}
