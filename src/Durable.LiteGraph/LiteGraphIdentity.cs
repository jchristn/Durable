namespace Durable.LiteGraph
{
    using System;
    using System.Globalization;
    using System.Security.Cryptography;
    using System.Text;

    /// <summary>
    /// Deterministic GUIDs for nodes and edges. A node GUID is derived from the graph GUID, the entity's table name and
    /// the canonical text of its stored key values, so a key lookup is a lookup by node GUID and the store's unique node
    /// GUIDs enforce unique primary keys. An edge GUID is derived from the graph GUID, the relationship and the dependent
    /// node GUID, so each foreign key has at most one edge. GUIDs are the first 16 bytes of a SHA-256 hash with the RFC 9562
    /// version 8 (custom) and variant bits set.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class LiteGraphIdentity
    {
        #region Public-Methods

        /// <summary>
        /// Returns the node GUID of an entity row.
        /// </summary>
        /// <param name="graphGuid">Graph GUID.</param>
        /// <param name="table">Entity table name. Must not be null.</param>
        /// <param name="storedKey">Stored key values in key order. Must not be null; values must not be null.</param>
        /// <returns>The GUID.</returns>
        /// <exception cref="ArgumentException">Thrown when a key value is null.</exception>
        public static Guid Node(Guid graphGuid, string table, object?[] storedKey)
        {
            StringBuilder text = new StringBuilder();
            text.Append("node\n").Append(graphGuid.ToString("N")).Append('\n').Append(table);
            foreach (object? value in storedKey)
            {
                if (value == null) throw new ArgumentException("Key values cannot be null.", nameof(storedKey));
                text.Append('\u001f').Append(KeyText(value));
            }

            return Hash(text.ToString());
        }

        /// <summary>
        /// Returns the edge GUID of a relationship from a dependent node.
        /// </summary>
        /// <param name="graphGuid">Graph GUID.</param>
        /// <param name="relationshipId">Relationship identifier. Must not be null.</param>
        /// <param name="dependentNode">Dependent node GUID.</param>
        /// <returns>The GUID.</returns>
        public static Guid Edge(Guid graphGuid, string relationshipId, Guid dependentNode)
        {
            return Hash("edge\n" + graphGuid.ToString("N") + "\n" + relationshipId + "\n" + dependentNode.ToString("N"));
        }

        /// <summary>
        /// Returns the canonical text of a stored key value: equal values (as <see cref="Durable.Query.QueryValueComparer"/>
        /// compares keys of the same column) have equal text.
        /// </summary>
        /// <param name="value">Stored value. Must not be null.</param>
        /// <returns>The text. Never null.</returns>
        public static string KeyText(object value)
        {
            switch (value)
            {
                case string s: return "s:" + s;
                case char c: return "s:" + c.ToString();
                case bool b: return b ? "b:1" : "b:0";
                case byte n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case sbyte n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case short n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case ushort n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case int n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case uint n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case long n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case ulong n: return "i:" + n.ToString(CultureInfo.InvariantCulture);
                case decimal m: return m == decimal.Truncate(m) && m >= long.MinValue && m <= long.MaxValue ? "i:" + ((long)m).ToString(CultureInfo.InvariantCulture) : "m:" + m.ToString("G29", CultureInfo.InvariantCulture);
                case double d: return d == Math.Truncate(d) && Math.Abs(d) < 9.2e18 ? "i:" + ((long)d).ToString(CultureInfo.InvariantCulture) : "m:" + d.ToString("R", CultureInfo.InvariantCulture);
                case float f: return f == Math.Truncate(f) && Math.Abs(f) < 9.2e18f ? "i:" + ((long)f).ToString(CultureInfo.InvariantCulture) : "m:" + ((double)f).ToString("R", CultureInfo.InvariantCulture);
                case Guid g: return "g:" + g.ToString("N");
                case DateTime dt: return "t:" + dt.Ticks.ToString(CultureInfo.InvariantCulture);
                case DateTimeOffset dto: return "o:" + dto.UtcTicks.ToString(CultureInfo.InvariantCulture);
                case TimeSpan span: return "p:" + span.Ticks.ToString(CultureInfo.InvariantCulture);
                case DateOnly date: return "d:" + date.DayNumber.ToString(CultureInfo.InvariantCulture);
                case TimeOnly time: return "h:" + time.Ticks.ToString(CultureInfo.InvariantCulture);
                case byte[] bytes: return "x:" + Convert.ToBase64String(bytes);
                default: return "v:" + Convert.ToString(value, CultureInfo.InvariantCulture);
            }
        }

        #endregion

        #region Private-Methods

        private static Guid Hash(string text)
        {
            byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));
            byte[] bytes = new byte[16];
            Array.Copy(hash, bytes, 16);
            bytes[6] = (byte)((bytes[6] & 0x0F) | 0x80);
            bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
            return new Guid(bytes, true);
        }

        #endregion
    }
}
