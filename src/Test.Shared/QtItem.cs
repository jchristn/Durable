namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Item entity for the query translation suites (table <c>qt_items</c>). Covers string, nullable, enum (string and
    /// integer storage), boolean, numeric and date columns plus a reference navigation to <see cref="QtOwner"/>.
    /// The table is created in-suite through <c>InitializeTable</c>.
    /// </summary>
    [Entity("qt_items")]
    public class QtItem
    {
        /// <summary>
        /// Gets or sets the primary key.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the item name. Never null; unique within the seed data.
        /// </summary>
        [Property("name", Flags.String, 128)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets a code containing special characters. May be null.
        /// </summary>
        [Property("code", Flags.String, 128)]
        public string? Code { get; set; }

        /// <summary>
        /// Gets or sets the email. May be null.
        /// </summary>
        [Property("email", Flags.String, 128)]
        public string? Email { get; set; }

        /// <summary>
        /// Gets or sets the status (stored as a string).
        /// </summary>
        [Property("status", Flags.String, 32)]
        public QtStatus Status { get; set; }

        /// <summary>
        /// Gets or sets the priority (stored as an integer).
        /// </summary>
        [Property("priority", Flags.Integer)]
        public QtPriority Priority { get; set; }

        /// <summary>
        /// Gets or sets the price.
        /// </summary>
        [Property("price")]
        public decimal Price { get; set; }

        /// <summary>
        /// Gets or sets a floating point ratio.
        /// </summary>
        [Property("ratio")]
        public double Ratio { get; set; }

        /// <summary>
        /// Gets or sets the quantity.
        /// </summary>
        [Property("quantity")]
        public int Quantity { get; set; }

        /// <summary>
        /// Gets or sets an optional discount. May be null.
        /// </summary>
        [Property("discount")]
        public int? Discount { get; set; }

        /// <summary>
        /// Gets or sets whether the item is active.
        /// </summary>
        [Property("is_active")]
        public bool IsActive { get; set; }

        /// <summary>
        /// Gets or sets an optional featured flag. May be null.
        /// </summary>
        [Property("is_featured")]
        public bool? IsFeatured { get; set; }

        /// <summary>
        /// Gets or sets the creation timestamp (UTC).
        /// </summary>
        [Property("created_utc")]
        public DateTime CreatedUtc { get; set; }

        /// <summary>
        /// Gets or sets an optional due date. May be null.
        /// </summary>
        [Property("due_date")]
        public DateTime? DueDate { get; set; }

        /// <summary>
        /// Gets or sets the category. Never null.
        /// </summary>
        [Property("category", Flags.String, 32)]
        public string Category { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the owning <see cref="QtOwner"/> key. May be null.
        /// </summary>
        [Property("owner_id")]
        public int? OwnerId { get; set; }

        /// <summary>
        /// Gets or sets the owner navigation. May be null.
        /// </summary>
        [NavigationProperty("OwnerId")]
        public QtOwner? Owner { get; set; }
    }
}
