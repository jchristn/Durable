namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Soft-deletable note belonging to a <see cref="CfAuthor"/>: deletes set <see cref="IsDeleted"/> instead of removing
    /// the row. Storage: <c>cf_notes</c>.
    /// </summary>
    [Entity("cf_notes")]
    public class CfNote
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the author key.</summary>
        [Property("author_id")]
        [ForeignKey(typeof(CfAuthor), "Id")]
        public int AuthorId { get; set; }

        /// <summary>Gets or sets the text. Never null.</summary>
        [Property("text", Flags.String, 64)]
        public string Text { get; set; } = string.Empty;

        /// <summary>Gets or sets the soft-delete marker.</summary>
        [Property("is_deleted")]
        [SoftDelete]
        public bool IsDeleted { get; set; }
    }
}
