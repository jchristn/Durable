namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Cosmos DB test entity with a composite key whose first part is the partition key (configured in the test settings).
    /// </summary>
    [Entity("cdb_tenant_lines")]
    public class CosmosDbTenantLine
    {
        [Property("tenant_id", Flags.PrimaryKey | Flags.String, 64, KeyOrder = 0)]
        public string TenantId { get; set; } = string.Empty;

        [Property("line_no", Flags.PrimaryKey, KeyOrder = 1)]
        public int LineNo { get; set; }

        [Property("text", Flags.String, 100)]
        public string Text { get; set; } = string.Empty;
    }
}
