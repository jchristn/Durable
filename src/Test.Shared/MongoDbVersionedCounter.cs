namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// MongoDB test entity with an integer version column.
    /// </summary>
    [Entity("mdb_versioned_counters")]
    public class MongoDbVersionedCounter
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
