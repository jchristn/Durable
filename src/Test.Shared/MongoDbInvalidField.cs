namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// MongoDB test entity with a column name that is not a valid MongoDB field name (it contains a dot).
    /// </summary>
    [Entity("mdb_invalid_fields")]
    public class MongoDbInvalidField
    {
        /// <summary>
        /// Gets or sets the key.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets a value whose column name contains a dot.
        /// </summary>
        [Property("a.b")]
        public int Dotted { get; set; }
    }
}
