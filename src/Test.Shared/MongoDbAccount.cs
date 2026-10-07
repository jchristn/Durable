namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// MongoDB test entity with a Guid key set by the caller, a unique nullable column and a unique composite index.
    /// </summary>
    [Entity("mdb_accounts")]
    [CompositeIndex("ux_accounts_tenant_handle", "tenant", "handle", IsUnique = true)]
    public class MongoDbAccount
    {
        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        [Property("id", Flags.PrimaryKey)]
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or sets the tenant.
        /// </summary>
        [Property("tenant")]
        public int Tenant { get; set; }

        /// <summary>
        /// Gets or sets the handle, unique per tenant.
        /// </summary>
        [Property("handle", Flags.String, 64)]
        public string? Handle { get; set; }

        /// <summary>
        /// Gets or sets the e-mail address, unique when set.
        /// </summary>
        [Property("email", Flags.String, 128)]
        [Index("ux_accounts_email", true)]
        public string? Email { get; set; }
    }
}
