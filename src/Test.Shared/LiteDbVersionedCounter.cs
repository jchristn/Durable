namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// LiteDB test entity with an integer version column.
    /// </summary>
    [Entity("ldb_versioned_counters")]
    public class LiteDbVersionedCounter
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
