namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity created by a reversible migration.
    /// </summary>
    [Entity("mig_down_items")]
    public class MigDownItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        [Property("name", Flags.String, 50)]
        public string? Name { get; set; }
    }
}
