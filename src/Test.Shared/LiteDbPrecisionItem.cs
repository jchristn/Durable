namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// LiteDB test entity covering every scalar type the backend encodes, keyed by a case-sensitive string code.
    /// </summary>
    [Entity("ldb_precision_items")]
    public class LiteDbPrecisionItem
    {
        [Property("code", Flags.PrimaryKey | Flags.String, 64)]
        public string Code { get; set; } = string.Empty;

        [Property("when")]
        public DateTime When { get; set; }

        [Property("when_nullable")]
        public DateTime? WhenNullable { get; set; }

        [Property("moment")]
        public DateTimeOffset Moment { get; set; }

        [Property("amount")]
        public decimal Amount { get; set; }

        [Property("ratio")]
        public double Ratio { get; set; }

        [Property("single")]
        public float Single { get; set; }

        [Property("token")]
        public Guid Token { get; set; }

        [Property("duration")]
        public TimeSpan Duration { get; set; }

        [Property("day")]
        public DateOnly Day { get; set; }

        [Property("time")]
        public TimeOnly Time { get; set; }

        [Property("payload")]
        public byte[]? Payload { get; set; }

        [Property("letter")]
        public char Letter { get; set; }

        [Property("big")]
        public ulong Big { get; set; }

        [Property("unsigned")]
        public uint Unsigned { get; set; }

        [Property("small")]
        public short Small { get; set; }

        [Property("tiny")]
        public byte Tiny { get; set; }

        [Property("signed_tiny")]
        public sbyte SignedTiny { get; set; }

        [Property("unsigned_small")]
        public ushort UnsignedSmall { get; set; }

        [Property("long_value")]
        public long LongValue { get; set; }

        [Property("flag")]
        public bool? Flag { get; set; }

        [Property("status", Flags.String, 32)]
        public Status Status { get; set; }

        [Property("status_number", Flags.Integer)]
        public Status StatusNumber { get; set; }
    }
}
