namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity: first version of the widgets table.
    /// </summary>
    [Entity("mig_widgets")]
    public class MigWidgetV1
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;
    }
}
