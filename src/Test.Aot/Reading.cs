namespace Test.Aot
{
    using System;
    using Durable;

    /// <summary>
    /// Wide-ish row used for the 10k-row materialization timing.
    /// </summary>
    [Entity("readings")]
    public class Reading
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("sensor", Flags.String, 64)]
        public string Sensor { get; set; } = string.Empty;

        [Property("value")]
        public double Value { get; set; }

        [Property("amount")]
        public decimal Amount { get; set; }

        [Property("taken")]
        public DateTime Taken { get; set; }

        [Property("active")]
        public bool Active { get; set; }

        [Property("status")]
        public AuthorStatus Status { get; set; }

        [Property("note", Flags.String, 200)]
        public string? Note { get; set; }

        [Property("external_id")]
        public Guid ExternalId { get; set; }
    }
}
