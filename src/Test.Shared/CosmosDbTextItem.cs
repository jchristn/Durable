namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Cosmos DB test entity whose string primary key column is named id, so it is the document id itself.
    /// </summary>
    [Entity("cdb_text_items")]
    public class CosmosDbTextItem
    {
        [Property("id", Flags.PrimaryKey | Flags.String, 200)]
        public string Id { get; set; } = string.Empty;

        [Property("text", Flags.String, 200)]
        public string? Text { get; set; }
    }
}
