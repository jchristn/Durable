namespace Test.Shared
{
    /// <summary>
    /// Mapping-source test tag.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_tags")]
    public class MsTag
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("tag_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the label.</summary>
        [MapColumn("label", Length = 32)]
        public string Label { get; set; } = string.Empty;
    }
}
