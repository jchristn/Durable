namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Soft-deletable note with a boolean marker.
    /// </summary>
    [Entity("rel_soft_notes")]
    public class RelSoftNote
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the parent identifier.
        /// </summary>
        [Property("parent_id")]
        public int ParentId { get; set; }

        /// <summary>
        /// Gets or sets the text. Never null.
        /// </summary>
        [Property("text", Flags.String, 100)]
        public string Text { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether the note is soft-deleted.
        /// </summary>
        [Property("is_deleted")]
        [SoftDelete]
        public bool IsDeleted { get; set; }
    }
}
