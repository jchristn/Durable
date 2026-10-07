namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Cosmos DB test entity: the owner side of a one-to-many relationship (auto-increment key in a column named id).
    /// </summary>
    [Entity("cdb_owners")]
    public class CosmosDbOwner
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        [InverseNavigationProperty("OwnerId")]
        public List<CosmosDbNote> Notes { get; set; } = new List<CosmosDbNote>();
    }
}
