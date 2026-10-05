namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Convention-mapped entity (no attributes except <see cref="NotMappedAttribute"/>): storage name
    /// <c>CfConventionWidget</c>, key <see cref="Id"/> (identity), columns named after the properties.
    /// </summary>
    public class CfConventionWidget
    {
        /// <summary>Gets or sets the convention key (generated identity).</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the quantity.</summary>
        public int Quantity { get; set; }

        /// <summary>Gets or sets the price.</summary>
        public decimal Price { get; set; }

        /// <summary>Gets or sets an optional note; may be null.</summary>
        public string? Notes { get; set; }

        /// <summary>Gets or sets a value that is never stored.</summary>
        [NotMapped]
        public string? Computed { get; set; }
    }
}
