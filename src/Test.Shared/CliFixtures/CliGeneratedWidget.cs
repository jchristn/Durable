namespace Test.Shared.CliFixtures.Generated
{
    using Durable;

    /// <summary>
    /// CLI fixture entity that 'durable migrations add' generates a migration for (--entities-namespace
    /// Test.Shared.CliFixtures.Generated).
    /// </summary>
    [Entity("cli_gen_widgets")]
    public class CliGeneratedWidget
    {
        /// <summary>Gets or sets the key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        [Property("name", Flags.String, 50)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the quantity.</summary>
        [Property("quantity")]
        [Index("idx_cli_gen_widgets_quantity")]
        public int Quantity { get; set; }

        /// <summary>Gets or sets the optional description.</summary>
        [Property("description")]
        public string? Description { get; set; }
    }
}
