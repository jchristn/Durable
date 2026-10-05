namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity: original column definitions of the typed table.
    /// </summary>
    [Entity("mig_typed")]
    public class MigTypedV1
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the count (integer).
        /// </summary>
        [Property("count")]
        public int Count { get; set; }

        /// <summary>
        /// Gets or sets the name (max 50).
        /// </summary>
        [Property("name", Flags.String, 50)]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the note (NOT NULL). Never null.
        /// </summary>
        [Property("note", Flags.String, 100)]
        public string Note { get; set; } = string.Empty;
    }
}
