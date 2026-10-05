namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity: wide version of the trim table with an extra indexed column.
    /// </summary>
    [Entity("mig_trim")]
    public class MigTrimFull
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

        /// <summary>
        /// Gets or sets a column dropped by the slim version.
        /// </summary>
        [Property("extra", Flags.String, 50)]
        [Index("idx_mig_trim_extra")]
        public string? Extra { get; set; }
    }
}
