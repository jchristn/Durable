namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Soft-deletable folder referenced by <see cref="CfDocument"/>. Storage: <c>cf_folders</c>.
    /// </summary>
    [Entity("cf_folders")]
    public class CfFolder
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the soft-delete marker.</summary>
        [Property("is_deleted")]
        [SoftDelete]
        public bool IsDeleted { get; set; }
    }
}
