namespace Test.Shared
{
    /// <summary>
    /// Mapping-source test class mapped by a source set as DurableMapping.MappingSource rather than registered for the type.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_global_items")]
    public class MsGlobalItem
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("global_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the label.</summary>
        [MapColumn("global_label", Length = 32)]
        public string Label { get; set; } = string.Empty;
    }
}
