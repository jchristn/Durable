namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Parent of soft-deletable notes. <see cref="Notes"/> deliberately has no initializer so that Include must
    /// assign an empty list when there are no children.
    /// </summary>
    [Entity("rel_soft_parents")]
    public class RelSoftParent
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
        /// Gets or sets the notes. Null until loaded with Include.
        /// </summary>
        [InverseNavigationProperty("ParentId")]
        public List<RelSoftNote>? Notes { get; set; }
    }
}
