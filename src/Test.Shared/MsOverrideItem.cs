namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Mapping-source test class that also carries Durable attributes; its registered source wins.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [Entity("ms_override_by_attribute")]
    [MapTable("ms_override_items")]
    public class MsOverrideItem
    {
        /// <summary>Gets or sets the key.</summary>
        [Property("attr_id", Flags.PrimaryKey | Flags.AutoIncrement)]
        [MapColumn("item_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        [Property("attr_name")]
        [MapColumn("item_name")]
        public string? Name { get; set; }
    }
}
