namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity created by the successful migration of the failure test.
    /// </summary>
    [Entity("mig_fail_items")]
    public class MigFailItem
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
