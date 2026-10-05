namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity: slim version of the trim table (no extra column or index).
    /// </summary>
    [Entity("mig_trim")]
    public class MigTrimSlim
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets a column kept by both versions.
        /// </summary>
        [Property("keep", Flags.String, 50)]
        public string? Keep { get; set; }
    }
}
