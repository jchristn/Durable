namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// MongoDB test entity whose table name is not a valid MongoDB collection name.
    /// </summary>
    [Entity("mdb$invalid")]
    public class MongoDbInvalidName
    {
        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }
    }
}
