namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Multi-tenant entity used to exercise global query filters.
    /// </summary>
    [Entity("rel_tenant_notes")]
    public class RelTenantNote
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        [Property("tenant_id")]
        public int TenantId { get; set; }

        /// <summary>
        /// Gets or sets the title. Never null.
        /// </summary>
        [Property("title", Flags.String, 100)]
        public string Title { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the amount.
        /// </summary>
        [Property("amount")]
        public int Amount { get; set; }
    }
}
