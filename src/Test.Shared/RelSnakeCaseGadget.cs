namespace Test.Shared
{
    using System;

    /// <summary>
    /// Convention-mapped entity used only under <c>DurableMapping.NamingConvention = SnakeCase</c>. Its metadata must
    /// be first built while snake case is active, so no other suite may reference this type.
    /// Maps to table rel_snake_case_gadget with columns id, display_name, unit_price, is_active, created_utc.
    /// </summary>
    public class RelSnakeCaseGadget
    {
        /// <summary>
        /// Gets or sets the identifier (convention key).
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the display name. Never null.
        /// </summary>
        public string DisplayName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the unit price.
        /// </summary>
        public decimal UnitPrice { get; set; }

        /// <summary>
        /// Gets or sets whether the gadget is active.
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// Gets or sets the creation time.
        /// </summary>
        public DateTime CreatedUtc { get; set; }
    }
}
