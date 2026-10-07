namespace Durable.CosmosDb
{
    /// <summary>
    /// How a stored value is represented in a Cosmos DB document (see <see cref="CosmosDbJsonCodec"/>), which decides how
    /// it is compared, ordered and aggregated inside Cosmos DB.
    /// </summary>
    internal enum CosmosDbValueKind
    {
        /// <summary>A JSON number that is always an exact integer within plus or minus 2^53 (int, uint, short, ushort, byte, sbyte, TimeOnly ticks).</summary>
        Integer,

        /// <summary>A JSON integer that may exceed 2^53 (long, ulong, TimeSpan ticks); larger values also get an exact shadow.</summary>
        Int64,

        /// <summary>A JSON number for a decimal; values a double cannot round-trip also get an exact shadow.</summary>
        Decimal,

        /// <summary>A JSON number for a float (shortest round-trip text); NaN and infinities are the strings "NaN", "Infinity" and "-Infinity".</summary>
        Single,

        /// <summary>A JSON number for a double (shortest round-trip text); NaN and infinities are the strings "NaN", "Infinity" and "-Infinity".</summary>
        Double,

        /// <summary>A JSON boolean.</summary>
        Boolean,

        /// <summary>A JSON string compared ordinally (string, char, enum names, JSON column text).</summary>
        String,

        /// <summary>A JSON string holding a Guid in lowercase "D" format (orders like <see cref="System.Guid.CompareTo(System.Guid)"/>).</summary>
        Guid,

        /// <summary>A JSON string holding the wall-clock ticks as fixed-width ISO 8601 text plus a kind suffix ("Z", none, or a local offset).</summary>
        DateTime,

        /// <summary>A JSON string holding the UTC instant as fixed-width ISO 8601 text ending in "Z"; a non-zero offset gets a shadow.</summary>
        DateTimeOffset,

        /// <summary>A JSON string "yyyy-MM-dd".</summary>
        DateOnly,

        /// <summary>A JSON string holding base64 bytes (equality only; never ordered inside Cosmos DB).</summary>
        Bytes
    }
}
