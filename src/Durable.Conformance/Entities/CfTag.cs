namespace Durable.Conformance
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Tag: many-to-many with <see cref="CfAuthor"/> through <see cref="CfAuthorTag"/>. Storage: <c>cf_tags</c>.
    /// </summary>
    [Entity("cf_tags")]
    public class CfTag
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the label. Never null.</summary>
        [Property("label", Flags.String, 64)]
        public string Label { get; set; } = string.Empty;

        /// <summary>Gets or sets the tagged authors (many-to-many, loaded by Include). Never null.</summary>
        [ManyToManyNavigationProperty(typeof(CfAuthorTag), "TagId", "AuthorId")]
        public List<CfAuthor> Authors { get; set; } = new List<CfAuthor>();
    }
}
