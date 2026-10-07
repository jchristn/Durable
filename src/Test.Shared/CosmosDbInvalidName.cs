namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Cosmos DB test entity whose table name is not a valid container name.
    /// </summary>
    [Entity("cdb/invalid")]
    public class CosmosDbInvalidName
    {
        [Property("id", Flags.PrimaryKey)]
        public int Id { get; set; }
    }
}
