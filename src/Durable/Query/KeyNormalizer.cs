namespace Durable.Query
{
    using System;
    using System.Globalization;

    /// <summary>
    /// Normalizes key values so keys read from different columns or drivers compare equal
    /// (for example, an <see cref="int"/> foreign key and a <see cref="long"/> primary key).
    /// Thread safety: stateless.
    /// </summary>
    public static class KeyNormalizer
    {
        /// <summary>
        /// Normalizes a key value: integral numbers become <see cref="long"/>, strings are compared ordinally,
        /// other values are returned unchanged.
        /// </summary>
        /// <param name="value">Key value; may be null.</param>
        /// <returns>The normalized value, or null.</returns>
        public static object? Normalize(object? value)
        {
            switch (value)
            {
                case null:
                    return null;
                case int i: return (long)i;
                case long l: return l;
                case short s: return (long)s;
                case byte b: return (long)b;
                case sbyte sb: return (long)sb;
                case uint ui: return (long)ui;
                case ushort us: return (long)us;
                case ulong ul when ul <= long.MaxValue: return (long)ul;
                case decimal d when d == decimal.Truncate(d) && d >= long.MinValue && d <= long.MaxValue: return decimal.ToInt64(d);
                case Enum e: return Convert.ToInt64(e, CultureInfo.InvariantCulture);
                default:
                    return value;
            }
        }
    }
}
