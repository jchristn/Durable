namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Oracle-specific test entity with mixed-case table and column names, used with
    /// <c>new OracleDialect(upperCaseIdentifiers: false)</c>.
    /// </summary>
    [Entity("OraMixedCase")]
    public class OraMixedCaseItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("ItemId", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int ItemId { get; set; }

        /// <summary>
        /// Gets or sets the display name. Never null.
        /// </summary>
        [Property("DisplayName", Flags.String, 50)]
        public string DisplayName { get; set; } = string.Empty;
    }
}
