namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity: second version of the widgets table, adding nullable, defaulted and undefaultable NOT NULL columns and an index.
    /// </summary>
    [Entity("mig_widgets")]
    public class MigWidgetV2
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
        /// Gets or sets the optional e-mail address (indexed).
        /// </summary>
        [Property("email", Flags.String, 120)]
        [Index("idx_mig_widgets_email")]
        public string? Email { get; set; }

        /// <summary>
        /// Gets or sets the quantity (NOT NULL; CLR default 0 applies to existing rows).
        /// </summary>
        [Property("quantity")]
        public int Quantity { get; set; }

        /// <summary>
        /// Gets or sets whether the widget is active (NOT NULL; DEFAULT true from the attribute).
        /// </summary>
        [Property("is_active")]
        [DefaultValue(DefaultValueType.True)]
        public bool IsActive { get; set; }

        /// <summary>
        /// Gets or sets the label (NOT NULL without a derivable default).
        /// </summary>
        [Property("label", Flags.String, 40)]
        public string Label { get; set; } = string.Empty;
    }
}
