namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Convention-mapped entity: no Entity or Property attributes. The table name is the type name and the
    /// key is the property named Id. <see cref="Computed"/> is excluded with <see cref="NotMappedAttribute"/> and
    /// <see cref="Labels"/> is skipped because it is not a scalar type.
    /// </summary>
    public class RelConventionWidget
    {
        /// <summary>
        /// Gets or sets the identifier (convention key, auto-increment).
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the quantity.
        /// </summary>
        public int Quantity { get; set; }

        /// <summary>
        /// Gets or sets the price.
        /// </summary>
        public decimal Price { get; set; }

        /// <summary>
        /// Gets or sets the creation time.
        /// </summary>
        public DateTime CreatedUtc { get; set; }

        /// <summary>
        /// Gets or sets optional notes. May be null.
        /// </summary>
        public string? Notes { get; set; }

        /// <summary>
        /// Gets or sets a value that must not be persisted. May be null.
        /// </summary>
        [NotMapped]
        public string? Computed { get; set; }

        /// <summary>
        /// Gets or sets a non-scalar value that convention mapping skips. Never null.
        /// </summary>
        public List<string> Labels { get; set; } = new List<string>();
    }
}
