namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// LiteDB test entity: auto-increment key, an indexed bounded string, a nullable number, a foreign key and a date.
    /// </summary>
    [Entity("ldb_notes")]
    public class LiteDbNote
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
        [ForeignKey(typeof(LiteDbOwner), "Id")]
        public int? OwnerId { get; set; }

        [Property("created")]
        public DateTime Created { get; set; }

        [NavigationProperty("OwnerId")]
        public LiteDbOwner? Owner { get; set; }
    }
}
