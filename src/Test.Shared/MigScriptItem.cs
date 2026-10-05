namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity used by the script generation tests (never created by them).
    /// </summary>
    [Entity("mig_script_items")]
    public class MigScriptItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the title (indexed).
        /// </summary>
        [Property("title", Flags.String, 80)]
        [Index("idx_mig_script_items_title")]
        public string? Title { get; set; }
    }
}
