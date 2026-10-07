namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// MongoDB test entity: the owner of <see cref="MongoDbNote"/>s.
    /// </summary>
    [Entity("mdb_owners")]
    public class MongoDbOwner
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        [InverseNavigationProperty("OwnerId")]
        public List<MongoDbNote> Notes { get; set; } = new List<MongoDbNote>();
    }
}
