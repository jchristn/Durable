namespace Test.Shared
{
    /// <summary>
    /// Mapping-source test class whose metadata is built before a source is registered for it.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_late_items")]
    public class MsLateItem
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("late_id", Key = true, Identity = true)]
        public int Id { get; set; }
    }
}
