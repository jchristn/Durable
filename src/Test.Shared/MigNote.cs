namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity created and seeded by versioned migrations.
    /// </summary>
    [Entity("mig_notes")]
    public class MigNote
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the body. Never null.
        /// </summary>
        [Property("body", Flags.String, 200)]
        public string Body { get; set; } = string.Empty;
    }
}
