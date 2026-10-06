namespace Durable.LiteGraph
{
    using System;
    using System.Globalization;
    using System.Text.Json;

    /// <summary>
    /// Encodes stored column values as JSON values in a node's data object and decodes them back, losslessly:
    /// strings, characters, booleans and Guids as JSON strings/booleans; integers and decimals as JSON numbers (decimals
    /// keep their scale); finite doubles and floats as round-trippable JSON numbers and non-finite ones as strings
    /// (<c>NaN</c>, <c>Infinity</c>, <c>-Infinity</c>); <see cref="DateTime"/> and <see cref="DateTimeOffset"/> in the
    /// round-trip "O" format (ticks and <see cref="DateTimeKind"/> or offset preserved); <see cref="TimeSpan"/> in the
    /// constant "c" format; <see cref="DateOnly"/> as <c>yyyy-MM-dd</c>; <see cref="TimeOnly"/> as
    /// <c>HH:mm:ss.fffffff</c>; byte arrays as base64. Decoding is driven by the column's stored type, so the encoding
    /// stays readable by LiteGraph tools while values round-trip exactly.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class LiteGraphJsonCodec
    {
        #region Public-Methods

        /// <summary>
        /// Writes a stored value.
        /// </summary>
        /// <param name="writer">Writer. Must not be null.</param>
        /// <param name="value">Stored value; may be null.</param>
        /// <exception cref="NotSupportedException">Thrown when the value's type has no JSON encoding.</exception>
        public static void Write(Utf8JsonWriter writer, object? value)
        {
            switch (value)
            {
                case null: writer.WriteNullValue(); return;
                case string s: writer.WriteStringValue(s); return;
                case char c: writer.WriteStringValue(c.ToString()); return;
                case bool b: writer.WriteBooleanValue(b); return;
                case byte n: writer.WriteNumberValue(n); return;
                case sbyte n: writer.WriteNumberValue(n); return;
                case short n: writer.WriteNumberValue(n); return;
                case ushort n: writer.WriteNumberValue(n); return;
                case int n: writer.WriteNumberValue(n); return;
                case uint n: writer.WriteNumberValue(n); return;
                case long n: writer.WriteNumberValue(n); return;
                case ulong n: writer.WriteNumberValue(n); return;
                case decimal m: writer.WriteNumberValue(m); return;
                case double d:
                    if (double.IsFinite(d)) writer.WriteNumberValue(d);
                    else writer.WriteStringValue(d.ToString("R", CultureInfo.InvariantCulture));
                    return;
                case float f:
                    if (float.IsFinite(f)) writer.WriteNumberValue(f);
                    else writer.WriteStringValue(f.ToString("R", CultureInfo.InvariantCulture));
                    return;
                case DateTime dateTime: writer.WriteStringValue(dateTime.ToString("O", CultureInfo.InvariantCulture)); return;
                case DateTimeOffset offset: writer.WriteStringValue(offset.ToString("O", CultureInfo.InvariantCulture)); return;
                case TimeSpan span: writer.WriteStringValue(span.ToString("c", CultureInfo.InvariantCulture)); return;
                case DateOnly date: writer.WriteStringValue(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)); return;
                case TimeOnly time: writer.WriteStringValue(time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture)); return;
                case Guid guid: writer.WriteStringValue(guid.ToString("D", CultureInfo.InvariantCulture)); return;
                case byte[] bytes: writer.WriteBase64StringValue(bytes); return;
                case Enum e: writer.WriteStringValue(e.ToString()); return;
                default:
                    throw new NotSupportedException("Values of type " + value.GetType().FullName + " cannot be stored in LiteGraph node data; use a value converter.");
            }
        }

        /// <summary>
        /// Reads a stored value of a given stored type.
        /// </summary>
        /// <param name="element">JSON value.</param>
        /// <param name="storedType">Stored type of the column (see <see cref="LiteGraphValueConverter.StoredType"/>). Must not be null.</param>
        /// <returns>The stored value, or null for JSON null.</returns>
        /// <exception cref="FormatException">Thrown when the JSON value cannot be read as the stored type.</exception>
        public static object? Read(JsonElement element, Type storedType)
        {
            if (element.ValueKind == JsonValueKind.Null || element.ValueKind == JsonValueKind.Undefined) return null;

            try
            {
                if (storedType == typeof(string)) return element.ValueKind == JsonValueKind.String ? element.GetString() : element.GetRawText();
                if (storedType == typeof(char))
                {
                    string text = Text(element);
                    return text.Length > 0 ? text[0] : '\0';
                }

                if (storedType == typeof(bool))
                {
                    if (element.ValueKind == JsonValueKind.True) return true;
                    if (element.ValueKind == JsonValueKind.False) return false;
                    if (element.ValueKind == JsonValueKind.Number) return element.GetDecimal() != 0m;
                    return bool.Parse(Text(element));
                }

                if (storedType == typeof(int)) return element.ValueKind == JsonValueKind.Number ? element.GetInt32() : int.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(long)) return element.ValueKind == JsonValueKind.Number ? element.GetInt64() : long.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(short)) return element.ValueKind == JsonValueKind.Number ? element.GetInt16() : short.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(byte)) return element.ValueKind == JsonValueKind.Number ? element.GetByte() : byte.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(sbyte)) return element.ValueKind == JsonValueKind.Number ? element.GetSByte() : sbyte.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(ushort)) return element.ValueKind == JsonValueKind.Number ? element.GetUInt16() : ushort.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(uint)) return element.ValueKind == JsonValueKind.Number ? element.GetUInt32() : uint.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(ulong)) return element.ValueKind == JsonValueKind.Number ? element.GetUInt64() : ulong.Parse(Text(element), NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (storedType == typeof(decimal)) return element.ValueKind == JsonValueKind.Number ? element.GetDecimal() : decimal.Parse(Text(element), NumberStyles.Float, CultureInfo.InvariantCulture);
                if (storedType == typeof(double)) return element.ValueKind == JsonValueKind.Number ? element.GetDouble() : double.Parse(Text(element), NumberStyles.Float, CultureInfo.InvariantCulture);
                if (storedType == typeof(float)) return element.ValueKind == JsonValueKind.Number ? element.GetSingle() : float.Parse(Text(element), NumberStyles.Float, CultureInfo.InvariantCulture);
                if (storedType == typeof(DateTime)) return DateTime.ParseExact(Text(element), "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                if (storedType == typeof(DateTimeOffset)) return DateTimeOffset.ParseExact(Text(element), "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                if (storedType == typeof(TimeSpan)) return TimeSpan.ParseExact(Text(element), "c", CultureInfo.InvariantCulture);
                if (storedType == typeof(DateOnly)) return DateOnly.ParseExact(Text(element), "yyyy-MM-dd", CultureInfo.InvariantCulture);
                if (storedType == typeof(TimeOnly)) return TimeOnly.ParseExact(Text(element), "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                if (storedType == typeof(Guid)) return Guid.Parse(Text(element));
                if (storedType == typeof(byte[])) return element.GetBytesFromBase64();
                if (storedType.IsEnum) return Enum.Parse(storedType, Text(element), true);
            }
            catch (Exception e) when (e is InvalidOperationException || e is FormatException || e is OverflowException || e is ArgumentException)
            {
                throw new FormatException("LiteGraph node data value " + element.GetRawText() + " cannot be read as " + storedType.Name + ".", e);
            }

            return Natural(element);
        }

        #endregion

        #region Private-Methods

        private static string Text(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.String ? element.GetString() ?? string.Empty : element.GetRawText();
        }

        private static object? Natural(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String: return element.GetString();
                case JsonValueKind.True: return true;
                case JsonValueKind.False: return false;
                case JsonValueKind.Number:
                    if (element.TryGetInt64(out long integer)) return integer;
                    if (element.TryGetDecimal(out decimal exact)) return exact;
                    return element.GetDouble();
                default: return element.GetRawText();
            }
        }

        #endregion
    }
}
