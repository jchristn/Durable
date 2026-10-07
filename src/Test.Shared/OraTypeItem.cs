namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Oracle-specific test entity covering the provider's type mapping: identity key, bounded and CLOB strings, NUMBER(1)
    /// booleans, RAW(16) GUIDs, BLOB bytes, INTERVAL durations, DATE days, TIMESTAMP WITH TIME ZONE and decimals.
    /// </summary>
    [Entity("ora_type_items")]
    public class OraTypeItem
    {
        /// <summary>
        /// Gets or sets the identifier (identity column).
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public long Id { get; set; }

        /// <summary>
        /// Gets or sets the name (VARCHAR2(64 CHAR)). Never null.
        /// </summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets an optional short code (VARCHAR2(20 CHAR)). May be null.
        /// </summary>
        [Property("code", Flags.String, 20)]
        public string? Code { get; set; }

        /// <summary>
        /// Gets or sets an optional long note (CLOB, maximum length above 4000). May be null.
        /// </summary>
        [Property("note", Flags.String, 100000)]
        public string? Note { get; set; }

        /// <summary>
        /// Gets or sets the flag (NUMBER(1)).
        /// </summary>
        [Property("flag")]
        public bool Flag { get; set; }

        /// <summary>
        /// Gets or sets the token (RAW(16)).
        /// </summary>
        [Property("token")]
        public Guid Token { get; set; }

        /// <summary>
        /// Gets or sets optional binary data (BLOB). May be null.
        /// </summary>
        [Property("data")]
        public byte[]? Data { get; set; }

        /// <summary>
        /// Gets or sets the duration (INTERVAL DAY(9) TO SECOND(7)).
        /// </summary>
        [Property("duration")]
        public TimeSpan Duration { get; set; }

        /// <summary>
        /// Gets or sets the day (DATE).
        /// </summary>
        [Property("day")]
        public DateOnly Day { get; set; }

        /// <summary>
        /// Gets or sets the time of day (INTERVAL DAY(0) TO SECOND(7)).
        /// </summary>
        [Property("time_of_day")]
        public TimeOnly TimeOfDay { get; set; }

        /// <summary>
        /// Gets or sets the instant with offset (TIMESTAMP(7) WITH TIME ZONE).
        /// </summary>
        [Property("happened")]
        public DateTimeOffset Happened { get; set; }

        /// <summary>
        /// Gets or sets the amount (NUMBER(38,10)).
        /// </summary>
        [Property("amount")]
        public decimal Amount { get; set; }
    }
}
