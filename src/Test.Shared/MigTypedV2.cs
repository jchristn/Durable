namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity: changed column definitions (type, length, nullability) of the typed table.
    /// </summary>
    [Entity("mig_typed")]
    public class MigTypedV2
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the count, now a string (type change).
        /// </summary>
        [Property("count", Flags.String, 20)]
        public string? Count { get; set; }

        /// <summary>
        /// Gets or sets the name, now max 100 (length change).
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the note, now nullable (nullability change).
        /// </summary>
        [Property("note", Flags.String, 100)]
        public string? Note { get; set; }
    }
}
