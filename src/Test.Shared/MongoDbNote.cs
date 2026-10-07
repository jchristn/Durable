namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// MongoDB test entity: auto-increment key, an indexed bounded string, a nullable number, a foreign key and a date.
    /// </summary>
    [Entity("mdb_notes")]
    public class MongoDbNote
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("title", Flags.String, 100)]
        [Index("ix_title")]
        public string Title { get; set; } = string.Empty;

        [Property("amount")]
        public int Amount { get; set; }

        [Property("rating")]
        public int? Rating { get; set; }

        [Property("owner_id")]
        [ForeignKey(typeof(MongoDbOwner), "Id")]
        public int? OwnerId { get; set; }

        [Property("created")]
        public DateTime Created { get; set; }

        [NavigationProperty("OwnerId")]
        public MongoDbOwner? Owner { get; set; }
    }
}
