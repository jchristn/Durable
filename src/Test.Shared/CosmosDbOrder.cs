namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Cosmos DB test entity partitioned by a non-key column (configured in the test settings).
    /// </summary>
    [Entity("cdb_orders")]
    public class CosmosDbOrder
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("tenant_id", Flags.String, 64)]
        public string TenantId { get; set; } = string.Empty;

        [Property("total")]
        public int Total { get; set; }
    }
}
