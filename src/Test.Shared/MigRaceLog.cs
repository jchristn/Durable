namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity recording which migration ran, for the concurrent migrator test.
    /// </summary>
    [Entity("mig_race_log")]
    public class MigRaceLog
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the migration that wrote the row. Never null.
        /// </summary>
        [Property("migration_id", Flags.String, 150)]
        public string MigrationId { get; set; } = string.Empty;
    }
}
