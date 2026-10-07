namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Cosmos DB test entity: auto-increment key, a string, numbers, a decimal, a long, a foreign key and a date.
    /// </summary>
    [Entity("cdb_notes")]
    public class CosmosDbNote
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("title", Flags.String, 100)]
        public string Title { get; set; } = string.Empty;

        [Property("amount")]
        public int Amount { get; set; }

        [Property("rating")]
        public int? Rating { get; set; }

        [Property("price")]
        public decimal Price { get; set; }

        [Property("big")]
        public long Big { get; set; }

        [Property("owner_id")]
        [ForeignKey(typeof(CosmosDbOwner), "Id")]
        public int? OwnerId { get; set; }

        [Property("created")]
        public DateTime Created { get; set; }

        [NavigationProperty("OwnerId")]
        public CosmosDbOwner? Owner { get; set; }
    }
}
