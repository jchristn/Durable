namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Every scalar type the LiteGraph backend stores, for exact round-trip tests. Storage: <c>lg_all_types</c>.
    /// </summary>
    [Entity("lg_all_types")]
    public class LgAllTypes
    {
        /// <summary>Gets or sets the generated 64-bit key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public long Id { get; set; }

        /// <summary>Gets or sets a string; may be null.</summary>
        [Property("text")]
        public string? Text { get; set; }

        /// <summary>Gets or sets a character.</summary>
        [Property("letter")]
        public char Letter { get; set; }

        /// <summary>Gets or sets a byte.</summary>
        [Property("tiny")]
        public byte Tiny { get; set; }

        /// <summary>Gets or sets a short.</summary>
        [Property("small")]
        public short Small { get; set; }

        /// <summary>Gets or sets a long.</summary>
        [Property("big")]
        public long Big { get; set; }

        /// <summary>Gets or sets an unsigned long.</summary>
        [Property("unsigned_big")]
        public ulong UnsignedBig { get; set; }

        /// <summary>Gets or sets a decimal.</summary>
        [Property("amount")]
        public decimal Amount { get; set; }

        /// <summary>Gets or sets a double.</summary>
        [Property("ratio")]
        public double Ratio { get; set; }

        /// <summary>Gets or sets a float.</summary>
        [Property("single")]
        public float Single { get; set; }

        /// <summary>Gets or sets a boolean.</summary>
        [Property("flag")]
        public bool Flag { get; set; }

        /// <summary>Gets or sets a UTC time.</summary>
        [Property("utc")]
        public DateTime Utc { get; set; }

        /// <summary>Gets or sets a local time.</summary>
        [Property("local")]
        public DateTime Local { get; set; }

        /// <summary>Gets or sets an unspecified-kind time.</summary>
        [Property("unspecified")]
        public DateTime Unspecified { get; set; }

        /// <summary>Gets or sets an optional time; may be null.</summary>
        [Property("maybe_time")]
        public DateTime? MaybeTime { get; set; }

        /// <summary>Gets or sets a time with offset.</summary>
        [Property("offset")]
        public DateTimeOffset Offset { get; set; }

        /// <summary>Gets or sets a duration.</summary>
        [Property("duration")]
        public TimeSpan Duration { get; set; }

        /// <summary>Gets or sets a date.</summary>
        [Property("day")]
        public DateOnly Day { get; set; }

        /// <summary>Gets or sets a time of day.</summary>
        [Property("time")]
        public TimeOnly Time { get; set; }

        /// <summary>Gets or sets a Guid.</summary>
        [Property("uid")]
        public Guid Uid { get; set; }

        /// <summary>Gets or sets bytes; may be null.</summary>
        [Property("blob")]
        public byte[]? Blob { get; set; }

        /// <summary>Gets or sets an enum stored by name.</summary>
        [Property("status")]
        public Status Status { get; set; }

        /// <summary>Gets or sets an enum stored as an integer.</summary>
        [Property("priority", Flags.Integer)]
        public Status Priority { get; set; }

        /// <summary>Gets or sets an optional integer; may be null.</summary>
        [Property("maybe")]
        public int? Maybe { get; set; }

        /// <summary>Gets or sets a JSON column; may be null.</summary>
        [Property("tags", Flags.Json)]
        public List<string>? Tags { get; set; }
    }
}
