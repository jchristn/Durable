namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Cosmos DB test entity with an integer version column.
    /// </summary>
    [Entity("cdb_counters")]
    public class CosmosDbCounter
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("counter")]
        public int Counter { get; set; }

        [Property("version")]
        [VersionColumn(VersionColumnType.Integer)]
        public int Version { get; set; }
    }
}
