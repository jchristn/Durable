namespace Durable.Conformance
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Owner of <see cref="CfItem"/> rows (collection navigation). Storage: <c>cf_owners</c>.
    /// </summary>
    [Entity("cf_owners")]
    public class CfOwner
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the city; may be null.</summary>
        [Property("city", Flags.String, 64)]
        public string? City { get; set; }

        /// <summary>Gets or sets the owned items (loaded by Include). Never null.</summary>
        [InverseNavigationProperty("OwnerId")]
        public List<CfItem> Items { get; set; } = new List<CfItem>();
    }
}
