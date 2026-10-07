namespace Test.Shared
{
    /// <summary>
    /// Mapping-source test class with a table but no columns, so it is mapped by convention; one property is ignored.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_convention_items")]
    public class MsConventionItem
    {
        /// <summary>Gets or sets the key (convention: Id).</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        public string? Name { get; set; }

        /// <summary>Gets or sets a value excluded from the mapping.</summary>
        [MapIgnore]
        public string? Secret { get; set; }
    }
}
