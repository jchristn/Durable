namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Single nullable string column used by string-matching-mode cases. Storage: <c>cf_text_items</c>.
    /// </summary>
    [Entity("cf_text_items")]
    public class CfTextItem
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the text; may be null.</summary>
        [Property("text", Flags.String, 64)]
        public string? Text { get; set; }
    }
}
