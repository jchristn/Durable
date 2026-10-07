namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity keyed by a custom type converted through a value converter.
    /// </summary>
    [Entity("reg_coded_items")]
    public class RegCodedItem
    {
        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        [Property("code", Flags.PrimaryKey | Flags.String, 32)]
        [ValueConverter(typeof(RegCodeConverter))]
        public RegCode Code { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 50)]
        public string Name { get; set; } = string.Empty;
    }
}
