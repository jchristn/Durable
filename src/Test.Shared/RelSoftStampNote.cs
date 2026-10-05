namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Soft-deletable note with a nullable timestamp marker.
    /// </summary>
    [Entity("rel_soft_stamp_notes")]
    public class RelSoftStampNote
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the text. Never null.
        /// </summary>
        [Property("text", Flags.String, 100)]
        public string Text { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets when the note was soft-deleted; null while live.
        /// </summary>
        [Property("deleted_utc")]
        [SoftDelete]
        public DateTime? DeletedUtc { get; set; }
    }
}
