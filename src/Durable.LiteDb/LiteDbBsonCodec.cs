namespace Durable.LiteDb
{
    using System;
    using System.Globalization;
    using LiteDB;

    /// <summary>
    /// Encodes stored values (see <see cref="LiteDbValueConverter"/>) as <see cref="BsonValue"/>s and decodes them back,
    /// losslessly. Encodings are chosen so that LiteDB orders and compares values of one stored type exactly as C# does:
    /// <list type="bullet">
    /// <item><description>string and char: BSON string; bool: boolean; Guid: guid; byte[]: binary.</description></item>
    /// <item><description>byte, sbyte, short, ushort, int: Int32; uint, long: Int64; ulong and decimal: Decimal (decimal keeps its scale); float and double: Double.</description></item>
    /// <item><description>DateTime: Decimal <c>Ticks + Kind / 4</c>, so ticks (the C# comparison key) order the values and the kind round-trips (LiteDB's own date type keeps only milliseconds in UTC).</description></item>
    /// <item><description>DateTimeOffset: Decimal <c>UtcTicks + (Offset.TotalMinutes + 840) / 2000</c>, ordered by the UTC instant like C#, keeping the offset.</description></item>
    /// <item><description>TimeSpan and TimeOnly: Int64 ticks; DateOnly: Int32 day number.</description></item>
    /// </list>
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class LiteDbBsonCodec
    {
        #region Public-Methods

        /// <summary>
        /// Encodes a stored value.
        /// </summary>
        /// <param name="value">Stored value; may be null.</param>
        /// <returns>The BSON value (<see cref="BsonValue.Null"/> for null). Never null.</returns>
        /// <exception cref="NotSupportedException">Thrown when the value's type has no BSON encoding.</exception>
        public static BsonValue Encode(object? value)
        {
            switch (value)
            {
                case null: return BsonValue.Null;
                case string s: return new BsonValue(s);
                case char c: return new BsonValue(c.ToString());
                case bool b: return new BsonValue(b);
                case int i: return new BsonValue(i);
                case long l: return new BsonValue(l);
                case short sh: return new BsonValue((int)sh);
                case byte by: return new BsonValue((int)by);
                case sbyte sb: return new BsonValue((int)sb);
                case ushort us: return new BsonValue((int)us);
                case uint ui: return new BsonValue((long)ui);
                case ulong ul: return new BsonValue((decimal)ul);
                case decimal m: return new BsonValue(m);
                case double d: return new BsonValue(d);
                case float f: return new BsonValue((double)f);
                case Guid g: return new BsonValue(g);
                case byte[] bytes: return new BsonValue((byte[])bytes.Clone());
                case DateTime dt: return new BsonValue(EncodeDateTime(dt));
                case DateTimeOffset dto: return new BsonValue(EncodeDateTimeOffset(dto));
                case TimeSpan ts: return new BsonValue(ts.Ticks);
                case DateOnly date: return new BsonValue(date.DayNumber);
                case TimeOnly time: return new BsonValue(time.Ticks);
                case Enum e: return new BsonValue(Convert.ToInt64(e, CultureInfo.InvariantCulture));
                default:
                    throw new NotSupportedException("The LiteDB backend cannot store values of type " + value.GetType().FullName + "; use a [ValueConverter] or a JSON column.");
            }
        }

        /// <summary>
        /// Decodes a BSON value into a stored value of a type.
        /// </summary>
        /// <param name="value">BSON value; null or <see cref="BsonValue.Null"/> decode to null.</param>
        /// <param name="storedType">Stored type (see <see cref="LiteDbValueConverter.StoredType"/>). Must not be null.</param>
        /// <returns>The stored value, or null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the BSON value cannot represent the stored type.</exception>
        public static object? Decode(BsonValue? value, Type storedType)
        {
            if (value == null || value.IsNull) return null;
            try
            {
                if (storedType == typeof(string)) return value.IsString ? value.AsString : Natural(value)?.ToString();
                if (storedType == typeof(char))
                {
                    string text = value.AsString;
                    return text.Length > 0 ? text[0] : '\0';
                }

                if (storedType == typeof(bool)) return value.AsBoolean;
                if (storedType == typeof(int)) return value.AsInt32;
                if (storedType == typeof(long)) return value.AsInt64;
                if (storedType == typeof(short)) return (short)value.AsInt32;
                if (storedType == typeof(byte)) return (byte)value.AsInt32;
                if (storedType == typeof(sbyte)) return (sbyte)value.AsInt32;
                if (storedType == typeof(ushort)) return (ushort)value.AsInt32;
                if (storedType == typeof(uint)) return (uint)value.AsInt64;
                if (storedType == typeof(ulong)) return (ulong)value.AsDecimal;
                if (storedType == typeof(decimal)) return value.AsDecimal;
                if (storedType == typeof(double)) return value.AsDouble;
                if (storedType == typeof(float)) return (float)value.AsDouble;
                if (storedType == typeof(Guid)) return value.AsGuid;
                if (storedType == typeof(byte[])) return value.AsBinary;
                if (storedType == typeof(DateTime)) return value.IsDateTime ? value.AsDateTime : DecodeDateTime(value.AsDecimal);
                if (storedType == typeof(DateTimeOffset)) return DecodeDateTimeOffset(value.AsDecimal);
                if (storedType == typeof(TimeSpan)) return new TimeSpan(value.AsInt64);
                if (storedType == typeof(DateOnly)) return DateOnly.FromDayNumber(value.AsInt32);
                if (storedType == typeof(TimeOnly)) return new TimeOnly(value.AsInt64);
                return Natural(value);
            }
            catch (Exception e) when (e is InvalidCastException || e is OverflowException || e is ArgumentException)
            {
                throw new InvalidOperationException("Stored LiteDB value " + value + " (" + value.Type + ") cannot be read as " + storedType.Name + ".", e);
            }
        }

        /// <summary>
        /// Returns whether values of a stored type are encoded so that LiteDB equality and ordering (between values of that
        /// type) are exactly C#'s: every encoded type except DateTime, DateTimeOffset (see <see cref="IsRanged"/>), byte
        /// arrays and strings (which depend on the database collation).
        /// </summary>
        /// <param name="storedType">Stored type. Must not be null.</param>
        /// <returns>True for directly comparable encodings.</returns>
        public static bool IsPlain(Type storedType)
        {
            return storedType == typeof(int) || storedType == typeof(long) || storedType == typeof(short) || storedType == typeof(byte)
                || storedType == typeof(sbyte) || storedType == typeof(ushort) || storedType == typeof(uint) || storedType == typeof(ulong)
                || storedType == typeof(decimal) || storedType == typeof(double) || storedType == typeof(float) || storedType == typeof(bool)
                || storedType == typeof(Guid) || storedType == typeof(TimeSpan) || storedType == typeof(DateOnly) || storedType == typeof(TimeOnly);
        }

        /// <summary>
        /// Returns whether a type is an integral number type (LiteDB compares integers of different widths exactly).
        /// </summary>
        /// <param name="type">Type. Must not be null.</param>
        /// <returns>True for byte, sbyte, short, ushort, int, uint, long and ulong.</returns>
        public static bool IsIntegral(Type type)
        {
            return type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)
                || type == typeof(sbyte) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong);
        }

        /// <summary>
        /// Returns whether a stored type is a string-like type (string, char) compared with the database collation.
        /// </summary>
        /// <param name="storedType">Stored type. Must not be null.</param>
        /// <returns>True for string and char.</returns>
        public static bool IsString(Type storedType)
        {
            return storedType == typeof(string) || storedType == typeof(char);
        }

        /// <summary>
        /// Returns whether a stored type is encoded as a decimal whose integral part is the C# comparison key and whose
        /// fraction carries extra information (DateTime kind, DateTimeOffset offset): equal C# values are a range of encoded values.
        /// </summary>
        /// <param name="storedType">Stored type. Must not be null.</param>
        /// <returns>True for DateTime and DateTimeOffset.</returns>
        public static bool IsRanged(Type storedType)
        {
            return storedType == typeof(DateTime) || storedType == typeof(DateTimeOffset);
        }

        /// <summary>
        /// Returns the comparison key of a ranged value: the ticks of a DateTime, the UTC ticks of a DateTimeOffset.
        /// </summary>
        /// <param name="value">DateTime or DateTimeOffset. Must not be null.</param>
        /// <returns>The key.</returns>
        /// <exception cref="ArgumentException">Thrown when the value is not ranged.</exception>
        public static decimal RangeKey(object value)
        {
            switch (value)
            {
                case DateTime dt: return dt.Ticks;
                case DateTimeOffset dto: return dto.UtcTicks;
                default: throw new ArgumentException("Value of type " + value.GetType().Name + " is not a ranged value.", nameof(value));
            }
        }

        #endregion

        #region Private-Methods

        private static decimal EncodeDateTime(DateTime value)
        {
            return value.Ticks + (int)value.Kind / 4m;
        }

        private static DateTime DecodeDateTime(decimal value)
        {
            decimal ticks = decimal.Truncate(value);
            int kind = (int)((value - ticks) * 4m);
            return new DateTime((long)ticks, (DateTimeKind)kind);
        }

        private static decimal EncodeDateTimeOffset(DateTimeOffset value)
        {
            return value.UtcTicks + ((decimal)value.Offset.TotalMinutes + 840m) / 2000m;
        }

        private static DateTimeOffset DecodeDateTimeOffset(decimal value)
        {
            decimal utcTicks = decimal.Truncate(value);
            int minutes = (int)((value - utcTicks) * 2000m) - 840;
            TimeSpan offset = TimeSpan.FromMinutes(minutes);
            return new DateTimeOffset((long)utcTicks + offset.Ticks, offset);
        }

        private static object? Natural(BsonValue value)
        {
            switch (value.Type)
            {
                case BsonType.Null: return null;
                case BsonType.Int32: return value.AsInt32;
                case BsonType.Int64: return value.AsInt64;
                case BsonType.Double: return value.AsDouble;
                case BsonType.Decimal: return value.AsDecimal;
                case BsonType.String: return value.AsString;
                case BsonType.Boolean: return value.AsBoolean;
                case BsonType.Guid: return value.AsGuid;
                case BsonType.Binary: return value.AsBinary;
                case BsonType.DateTime: return value.AsDateTime;
                case BsonType.ObjectId: return value.AsObjectId.ToString();
                default: throw new InvalidOperationException("Stored LiteDB value of type " + value.Type + " cannot be read as a column value.");
            }
        }

        #endregion
    }
}
