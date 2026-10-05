namespace Durable.Conformance
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Publisher: parent of <see cref="CfAuthor"/> (collection navigation) and of <see cref="CfBook"/>. Storage: <c>cf_publishers</c>.
    /// </summary>
    [Entity("cf_publishers")]
    public class CfPublisher
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the authors signed to this publisher (loaded by Include). Never null.</summary>
        [InverseNavigationProperty("PublisherId")]
        public List<CfAuthor> Authors { get; set; } = new List<CfAuthor>();
    }
}
