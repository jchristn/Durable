namespace Test.Shared.CliFixtures.Entities
{
    using Durable;

    /// <summary>
    /// CLI fixture entity used by the schema diff/sync tests (--entities-namespace Test.Shared.CliFixtures.Entities).
    /// </summary>
    [Entity("cli_customers")]
    public class CliCustomer
    {
        /// <summary>Gets or sets the key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        [Property("name", Flags.String, 100)]
        [Index("idx_cli_customers_name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the email address.</summary>
        [Property("email", Flags.String, 200)]
        public string? Email { get; set; }
    }
}
