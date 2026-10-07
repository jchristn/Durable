namespace Test.Shared.CliFixtures.Mapped
{
    using System;

    /// <summary>
    /// CLI fixture entity with no Durable attributes, mapped through --mapping-source <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("cli_mapped_gadgets")]
    [MapCompositeIndex("idx_cli_mapped_gadgets_kind_name", "kind", "gadget_name")]
    public class CliMappedGadget
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("gadget_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        [MapColumn("gadget_name", Length = 40)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the kind.</summary>
        [MapColumn("kind", Length = 16)]
        public string Kind { get; set; } = string.Empty;

        /// <summary>Gets or sets when the gadget was made.</summary>
        [MapColumn("made_utc")]
        public DateTime MadeUtc { get; set; }
    }
}
