namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// DuckDB test entity covering the native types the dialect maps (unsigned integers, UUID, TIMESTAMPTZ, INTERVAL,
    /// DATE, TIME, DECIMAL, BLOB, JSON) plus an indexed name.
    /// </summary>
    [Entity("duck_types_items")]
    public class DuckTypesItem
    {
        /// <summary>
        /// Gets or sets the identifier (sequence-backed).
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 40)]
        [Index("idx_duck_types_name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets an unsigned byte (UTINYINT).
        /// </summary>
        [Property("tiny")]
        public byte Tiny { get; set; }

        /// <summary>
        /// Gets or sets a signed byte (TINYINT).
        /// </summary>
        [Property("signed_tiny")]
        public sbyte SignedTiny { get; set; }

        /// <summary>
        /// Gets or sets an unsigned short (USMALLINT).
        /// </summary>
        [Property("small")]
        public ushort Small { get; set; }

        /// <summary>
        /// Gets or sets an unsigned int (UINTEGER).
        /// </summary>
        [Property("medium")]
        public uint Medium { get; set; }

        /// <summary>
        /// Gets or sets an unsigned long (UBIGINT).
        /// </summary>
        [Property("large")]
        public ulong Large { get; set; }

        /// <summary>
        /// Gets or sets a GUID (UUID).
        /// </summary>
        [Property("uid")]
        public Guid Uid { get; set; }

        /// <summary>
        /// Gets or sets an instant (TIMESTAMPTZ).
        /// </summary>
        [Property("at")]
        public DateTimeOffset At { get; set; }

        /// <summary>
        /// Gets or sets a local timestamp (TIMESTAMP).
        /// </summary>
        [Property("stamp")]
        public DateTime Stamp { get; set; }

        /// <summary>
        /// Gets or sets a duration (INTERVAL).
        /// </summary>
        [Property("span")]
        public TimeSpan Span { get; set; }

        /// <summary>
        /// Gets or sets a date (DATE).
        /// </summary>
        [Property("day")]
        public DateOnly Day { get; set; }

        /// <summary>
        /// Gets or sets a time of day (TIME).
        /// </summary>
        [Property("clock")]
        public TimeOnly Clock { get; set; }

        /// <summary>
        /// Gets or sets a decimal (DECIMAL(38,10)).
        /// </summary>
        [Property("price")]
        public decimal Price { get; set; }

        /// <summary>
        /// Gets or sets binary data (BLOB). May be null.
        /// </summary>
        [Property("data")]
        public byte[]? Data { get; set; }

        /// <summary>
        /// Gets or sets JSON tags (JSON). May be null.
        /// </summary>
        [Property("tags", Flags.Json)]
        public string[]? Tags { get; set; }
    }
}
