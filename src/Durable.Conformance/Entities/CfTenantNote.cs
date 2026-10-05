namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Multi-tenant row used by query-filter cases. Storage: <c>cf_tenant_notes</c>.
    /// </summary>
    [Entity("cf_tenant_notes")]
    public class CfTenantNote
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the tenant.</summary>
        [Property("tenant_id")]
        public int TenantId { get; set; }

        /// <summary>Gets or sets the title. Never null.</summary>
        [Property("title", Flags.String, 64)]
        public string Title { get; set; } = string.Empty;

        /// <summary>Gets or sets the amount.</summary>
        [Property("amount")]
        public int Amount { get; set; }
    }
}
