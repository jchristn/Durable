namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Entity whose columns are populated by default value providers on insert.
    /// </summary>
    [Entity("rel_defaulted_items")]
    public class RelDefaultedItem
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
        /// Gets or sets a token generated on insert when empty.
        /// </summary>
        [Property("token")]
        [DefaultValue(DefaultValueType.NewGuid)]
        public Guid Token { get; set; }

        /// <summary>
        /// Gets or sets the creation time, set to the current UTC time on insert when unset.
        /// </summary>
        [Property("created_utc")]
        [DefaultValue(DefaultValueType.CurrentDateTimeUtc)]
        public DateTime CreatedUtc { get; set; }
    }
}
