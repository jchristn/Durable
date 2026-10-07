namespace Durable.MongoDb
{
    using System;
    using System.Globalization;
    using MongoDB.Bson;

    /// <summary>
    /// Encodes stored values (see <see cref="MongoDbValueConverter"/>) as <see cref="BsonValue"/>s and decodes them back,
    /// losslessly. Encodings are chosen so that MongoDB orders and compares values of one stored type exactly as C# does:
    /// <list type="bullet">
    /// <item><description>string and char: BSON string; bool: boolean; Guid: binary subtype 4 (standard, big-endian, so byte order is <see cref="Guid.CompareTo(Guid)"/> order); byte[]: binary subtype 0.</description></item>
    /// <item><description>byte, sbyte, short, ushort, int: Int32; uint, long: Int64; ulong and decimal: Decimal128 (decimal keeps its scale); float and double: Double.</description></item>
    /// <item><description>DateTime: Decimal128 <c>Ticks + Kind / 4</c>, so ticks (the C# comparison key) order the values and every tick and the kind round-trip (the BSON date type keeps only milliseconds in UTC).</description></item>
    /// <item><description>DateTimeOffset: Decimal128 <c>UtcTicks + (Offset.TotalMinutes + 840) / 2000</c>, ordered by the UTC instant like C#, keeping the offset.</description></item>
    /// <item><description>TimeSpan and TimeOnly: Int64 ticks; DateOnly: Int32 day number.</description></item>
    /// </list>
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class MongoDbBsonCodec
    {
        #region Public-Methods

        /// <summary>
        /// Encodes a stored value.
        /// </summary>
        /// <param name="value">Stored value; may be null.</param>
        /// <returns>The BSON value (<see cref="BsonNull.Value"/> for null). Never null.</returns>
        /// <exception cref="NotSupportedException">Thrown when the value's type has no BSON encoding.</exception>
        public static BsonValue Encode(object? value)
        {
            switch (value)
            {
                case null: return BsonNull.Value;
                case string s: return new BsonString(s);
                case char c: return new BsonString(c.ToString());
                case bool b: return b ? BsonBoolean.True : BsonBoolean.False;
                case int i: return new BsonInt32(i);
                case long l: return new BsonInt64(l);
                case short sh: return new BsonInt32(sh);
                case byte by: return new BsonInt32(by);
                case sbyte sb: return new BsonInt32(sb);
                case ushort us: return new BsonInt32(us);
                case uint ui: return new BsonInt64(ui);
                case ulong ul: return new BsonDecimal128(new Decimal128(ul));
                case decimal m: return new BsonDecimal128(new Decimal128(m));
                case double d: return new BsonDouble(d);
                case float f: return new BsonDouble(f);
                case Guid g: return new BsonBinaryData(g, GuidRepresentation.Standard);
                case byte[] bytes: return new BsonBinaryData((byte[])bytes.Clone(), BsonBinarySubType.Binary);
                case DateTime dt: return new BsonDecimal128(new Decimal128(EncodeDateTime(dt)));
                case DateTimeOffset dto: return new BsonDecimal128(new Decimal128(EncodeDateTimeOffset(dto)));
                case TimeSpan ts: return new BsonInt64(ts.Ticks);
                case DateOnly date: return new BsonInt32(date.DayNumber);
                case TimeOnly time: return new BsonInt64(time.Ticks);
                case Enum e: return new BsonInt64(Convert.ToInt64(e, CultureInfo.InvariantCulture));
                default:
                    throw new NotSupportedException("The MongoDB backend cannot store values of type " + value.GetType().FullName + "; use a [ValueConverter] or a JSON column.");
            }
        }

        /// <summary>
        /// Encodes a decimal as a BSON Decimal128 (used for range bounds of DateTime and DateTimeOffset keys).
        /// </summary>
        /// <param name="value">Value.</param>
        /// <returns>The BSON value. Never null.</returns>
        public static BsonValue EncodeDecimal(decimal value)
        {
            return new BsonDecimal128(new Decimal128(value));
        }

        /// <summary>
        /// Decodes a BSON value into a stored value of a type.
        /// </summary>
        /// <param name="value">BSON value; null, <see cref="BsonNull"/> and <see cref="BsonUndefined"/> decode to null.</param>
        /// <param name="storedType">Stored type (see <see cref="MongoDbValueConverter.StoredType"/>). Must not be null.</param>
        /// <returns>The stored value, or null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the BSON value cannot represent the stored type.</exception>
        public static object? Decode(BsonValue? value, Type storedType)
        {
            if (value == null || value.IsBsonNull || value.IsBsonUndefined) return null;
            try
            {
                if (storedType == typeof(string)) return value.IsString ? value.AsString : Natural(value)?.ToString();
                if (storedType == typeof(char))
                {
                    string text = value.AsString;
                    return text.Length > 0 ? text[0] : '\0';
                }

                if (storedType == typeof(bool)) return value.AsBoolean;
                if (storedType == typeof(int)) return checked((int)Integral(value));
                if (storedType == typeof(long)) return Integral(value);
                if (storedType == typeof(short)) return checked((short)Integral(value));
                if (storedType == typeof(byte)) return checked((byte)Integral(value));
                if (storedType == typeof(sbyte)) return checked((sbyte)Integral(value));
                if (storedType == typeof(ushort)) return checked((ushort)Integral(value));
                if (storedType == typeof(uint)) return checked((uint)Integral(value));
                if (storedType == typeof(ulong)) return value.IsDecimal128 ? Decimal128.ToUInt64(value.AsDecimal128) : checked((ulong)Integral(value));
                if (storedType == typeof(decimal)) return ToDecimal(value);
                if (storedType == typeof(double)) return ToDouble(value);
                if (storedType == typeof(float)) return (float)ToDouble(value);
                if (storedType == typeof(Guid)) return value.AsBsonBinaryData.ToGuid(value.AsBsonBinaryData.SubType == BsonBinarySubType.UuidLegacy ? GuidRepresentation.CSharpLegacy : GuidRepresentation.Standard);
                if (storedType == typeof(byte[])) return (byte[])value.AsBsonBinaryData.Bytes.Clone();
                if (storedType == typeof(DateTime)) return value.IsValidDateTime ? value.ToUniversalTime() : DecodeDateTime(ToDecimal(value));
                if (storedType == typeof(DateTimeOffset)) return value.IsValidDateTime ? new DateTimeOffset(value.ToUniversalTime()) : DecodeDateTimeOffset(ToDecimal(value));
                if (storedType == typeof(TimeSpan)) return new TimeSpan(Integral(value));
                if (storedType == typeof(DateOnly)) return DateOnly.FromDayNumber(checked((int)Integral(value)));
                if (storedType == typeof(TimeOnly)) return new TimeOnly(Integral(value));
                return Natural(value);
            }
            catch (Exception e) when (e is InvalidCastException || e is OverflowException || e is ArgumentException || e is FormatException)
            {
                throw new InvalidOperationException("Stored MongoDB value " + value + " (" + value.BsonType + ") cannot be read as " + storedType.Name + ".", e);
            }
        }

        /// <summary>
        /// Converts a numeric BSON value (Int32, Int64, Double or Decimal128) to a decimal.
        /// </summary>
        /// <param name="value">Value. Must not be null.</param>
        /// <returns>The decimal.</returns>
        /// <exception cref="InvalidCastException">Thrown when the value is not numeric.</exception>
        /// <exception cref="OverflowException">Thrown when the value does not fit a decimal.</exception>
        public static decimal ToDecimal(BsonValue value)
        {
            switch (value.BsonType)
            {
                case BsonType.Int32: return value.AsInt32;
                case BsonType.Int64: return value.AsInt64;
                case BsonType.Double: return (decimal)value.AsDouble;
                case BsonType.Decimal128: return Decimal128.ToDecimal(value.AsDecimal128);
                default: throw new InvalidCastException("BSON " + value.BsonType + " is not a number.");
            }
        }

        /// <summary>
        /// Returns whether values of a stored type are encoded so that MongoDB equality and ordering (between values of that
        /// type) are exactly C#'s: every encoded type except DateTime, DateTimeOffset (see <see cref="IsRanged"/>), byte
        /// arrays (MongoDB orders binary data by length first) and strings and chars (see <see cref="IsString"/>).
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
        /// Returns whether a stored type is an exact number type whose sum MongoDB can compute exactly as Decimal128
        /// (integers, ulong and decimal; not float or double, whose sums depend on the order of addition).
        /// </summary>
        /// <param name="storedType">Stored type. Must not be null.</param>
        /// <returns>True for exact numeric types.</returns>
        public static bool IsExactNumber(Type storedType)
        {
            return storedType == typeof(int) || storedType == typeof(long) || storedType == typeof(short) || storedType == typeof(byte)
                || storedType == typeof(sbyte) || storedType == typeof(ushort) || storedType == typeof(uint) || storedType == typeof(ulong)
                || storedType == typeof(decimal);
        }

        /// <summary>
        /// Returns whether a stored type is a string-like type (string, char), compared by MongoDB without a collation (UTF-8
        /// bytes, which is Unicode code point order).
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

        private static long Integral(BsonValue value)
        {
            switch (value.BsonType)
            {
                case BsonType.Int32: return value.AsInt32;
                case BsonType.Int64: return value.AsInt64;
                case BsonType.Double: return checked((long)value.AsDouble);
                case BsonType.Decimal128: return Decimal128.ToInt64(value.AsDecimal128);
                default: throw new InvalidCastException("BSON " + value.BsonType + " is not an integer.");
            }
        }

        private static double ToDouble(BsonValue value)
        {
            switch (value.BsonType)
            {
                case BsonType.Double: return value.AsDouble;
                case BsonType.Int32: return value.AsInt32;
                case BsonType.Int64: return value.AsInt64;
                case BsonType.Decimal128: return Decimal128.ToDouble(value.AsDecimal128);
                default: throw new InvalidCastException("BSON " + value.BsonType + " is not a number.");
            }
        }

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
            int minutes = (int)decimal.Round((value - utcTicks) * 2000m) - 840;
            TimeSpan offset = TimeSpan.FromMinutes(minutes);
            return new DateTimeOffset((long)utcTicks + offset.Ticks, offset);
        }

        private static object? Natural(BsonValue value)
        {
            switch (value.BsonType)
            {
                case BsonType.Null:
                case BsonType.Undefined:
                    return null;
                case BsonType.Int32: return value.AsInt32;
                case BsonType.Int64: return value.AsInt64;
                case BsonType.Double: return value.AsDouble;
                case BsonType.Decimal128: return Decimal128.ToDecimal(value.AsDecimal128);
                case BsonType.String: return value.AsString;
                case BsonType.Boolean: return value.AsBoolean;
                case BsonType.Binary:
                    {
                        BsonBinaryData binary = value.AsBsonBinaryData;
                        if (binary.SubType == BsonBinarySubType.UuidStandard) return binary.ToGuid(GuidRepresentation.Standard);
                        return (byte[])binary.Bytes.Clone();
                    }
                case BsonType.DateTime: return value.ToUniversalTime();
                case BsonType.ObjectId: return value.AsObjectId.ToString();
                default: throw new InvalidOperationException("Stored MongoDB value of type " + value.BsonType + " cannot be read as a column value.");
            }
        }

        #endregion
    }
}
