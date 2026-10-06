namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// LiteDB test entity whose table name is not a valid LiteDB collection name.
    /// </summary>
    [Entity("ldb-invalid.name")]
    public class LiteDbInvalidName
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }
    }
}
