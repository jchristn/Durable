namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Entity keyed by a <see cref="Guid"/>.
    /// </summary>
    [Entity("reg_guid_items")]
    public class RegGuidItem
    {
        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        [Property("id", Flags.PrimaryKey)]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 50)]
        public string Name { get; set; } = string.Empty;
    }
}
