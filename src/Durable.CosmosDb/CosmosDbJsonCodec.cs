namespace Durable.CosmosDb
{
    using System;
    using System.Globalization;
    using System.Text.Json;
    using System.Text.Json.Nodes;

    /// <summary>
    /// Encodes stored values (see <see cref="CosmosDbValueConverter"/>) as JSON document values, losslessly and so that Cosmos
    /// DB compares and orders them like C#: integers, floats, doubles and decimals as JSON numbers (shortest round-trip text),
    /// bool as JSON booleans, strings, chars and Guids as strings, DateTime as fixed-width wall-clock text plus a kind suffix,
    /// DateTimeOffset as its fixed-width UTC instant, DateOnly as "yyyy-MM-dd", TimeOnly and TimeSpan as ticks, byte arrays
    /// as base64. Cosmos DB may keep JSON numbers only as doubles, so values a double cannot hold exactly (integers beyond
    /// plus or minus 2^53, decimals that do not round-trip through a double) also get an exact text "shadow", stored under the
    /// document's <c>_durable</c> object, and so does a non-zero DateTimeOffset offset; reads prefer the shadow.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class CosmosDbJsonCodec
    {
        #region Public-Members

        /// <summary>
        /// The largest magnitude of an integer a double holds exactly (2^53).
        /// </summary>
        public const long MaxExactInteger = 9007199254740992L;

        /// <summary>
        /// The character appended to a DateTime prefix to form the exclusive upper bound of every value with that prefix.
        /// </summary>
        public const char PrefixUpperBound = '\u007f';

        #endregion

        #region Private-Members

        private const string _DateTimeFormat = "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fffffff";
        private const int _DateTimePrefixLength = 27;
        private const string _DateOnlyFormat = "yyyy'-'MM'-'dd";
        private const string _NaN = "NaN";
        private const string _PositiveInfinity = "Infinity";
        private const string _NegativeInfinity = "-Infinity";

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the representation kind of a stored type.
        /// </summary>
        /// <param name="stored">Stored CLR type. Must not be null.</param>
        /// <returns>The kind.</returns>
        /// <exception cref="NotSupportedException">Thrown when the type cannot be stored in a Cosmos DB document.</exception>
        public static CosmosDbValueKind KindOf(Type stored)
        {
            if (stored == typeof(int) || stored == typeof(short) || stored == typeof(byte) || stored == typeof(sbyte)
                || stored == typeof(ushort) || stored == typeof(uint) || stored == typeof(TimeOnly)) return CosmosDbValueKind.Integer;
            if (stored == typeof(long) || stored == typeof(ulong) || stored == typeof(TimeSpan)) return CosmosDbValueKind.Int64;
            if (stored == typeof(decimal)) return CosmosDbValueKind.Decimal;
            if (stored == typeof(float)) return CosmosDbValueKind.Single;
            if (stored == typeof(double)) return CosmosDbValueKind.Double;
            if (stored == typeof(bool)) return CosmosDbValueKind.Boolean;
            if (stored == typeof(string) || stored == typeof(char)) return CosmosDbValueKind.String;
            if (stored == typeof(Guid)) return CosmosDbValueKind.Guid;
            if (stored == typeof(DateTime)) return CosmosDbValueKind.DateTime;
            if (stored == typeof(DateTimeOffset)) return CosmosDbValueKind.DateTimeOffset;
            if (stored == typeof(DateOnly)) return CosmosDbValueKind.DateOnly;
            if (stored == typeof(byte[])) return CosmosDbValueKind.Bytes;
            throw new NotSupportedException("Values of type " + stored.Name + " cannot be stored in a Cosmos DB document; use a value converter to a supported type.");
        }

        /// <summary>
        /// Returns whether values of a kind are JSON strings.
        /// </summary>
        /// <param name="kind">Kind.</param>
        /// <returns>True for string-represented kinds.</returns>
        public static bool IsStringKind(CosmosDbValueKind kind)
        {
            return kind == CosmosDbValueKind.String || kind == CosmosDbValueKind.Guid || kind == CosmosDbValueKind.DateTime
                || kind == CosmosDbValueKind.DateTimeOffset || kind == CosmosDbValueKind.DateOnly || kind == CosmosDbValueKind.Bytes;
        }

        /// <summary>
        /// Returns whether values of a kind are JSON numbers.
        /// </summary>
        /// <param name="kind">Kind.</param>
        /// <returns>True for number-represented kinds.</returns>
        public static bool IsNumberKind(CosmosDbValueKind kind)
        {
            return kind == CosmosDbValueKind.Integer || kind == CosmosDbValueKind.Int64 || kind == CosmosDbValueKind.Decimal
                || kind == CosmosDbValueKind.Single || kind == CosmosDbValueKind.Double;
        }

        /// <summary>
        /// Encodes a stored value as a JSON value.
        /// </summary>
        /// <param name="stored">Stored value; may be null.</param>
        /// <param name="kind">Kind of the column.</param>
        /// <param name="shadow">Receives the exact text to keep in the document's shadow object, or null when the JSON value is exact.</param>
        /// <returns>The JSON value, or null for a null value.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the value does not have the column's stored type.</exception>
        public static JsonNode? Encode(object? stored, CosmosDbValueKind kind, out string? shadow)
        {
            shadow = null;
            if (stored == null) return null;
            switch (stored)
            {
                case int i: return JsonValue.Create(i);
                case short s: return JsonValue.Create(s);
                case byte b: return JsonValue.Create(b);
                case sbyte sb: return JsonValue.Create(sb);
                case ushort us: return JsonValue.Create(us);
                case uint ui: return JsonValue.Create(ui);
                case TimeOnly time: return JsonValue.Create(time.Ticks);
                case long l:
                    if (l > MaxExactInteger || l < -MaxExactInteger) shadow = l.ToString(CultureInfo.InvariantCulture);
                    return JsonValue.Create(l);
                case ulong ul:
                    if (ul > (ulong)MaxExactInteger) shadow = ul.ToString(CultureInfo.InvariantCulture);
                    return JsonValue.Create(ul);
                case TimeSpan span:
                    if (span.Ticks > MaxExactInteger || span.Ticks < -MaxExactInteger) shadow = span.Ticks.ToString(CultureInfo.InvariantCulture);
                    return JsonValue.Create(span.Ticks);
                case decimal d:
                    if (!RoundTrips(d)) shadow = d.ToString(CultureInfo.InvariantCulture);
                    return JsonValue.Create(d);
                case float f:
                    if (float.IsNaN(f)) return JsonValue.Create(_NaN);
                    if (float.IsPositiveInfinity(f)) return JsonValue.Create(_PositiveInfinity);
                    if (float.IsNegativeInfinity(f)) return JsonValue.Create(_NegativeInfinity);
                    return JsonValue.Create(f);
                case double dbl:
                    if (double.IsNaN(dbl)) return JsonValue.Create(_NaN);
                    if (double.IsPositiveInfinity(dbl)) return JsonValue.Create(_PositiveInfinity);
                    if (double.IsNegativeInfinity(dbl)) return JsonValue.Create(_NegativeInfinity);
                    return JsonValue.Create(dbl);
                case bool flag: return JsonValue.Create(flag);
                case string text: return JsonValue.Create(text);
                case char c: return JsonValue.Create(c.ToString());
                case Guid guid: return JsonValue.Create(guid.ToString("D"));
                case DateTime dateTime: return JsonValue.Create(FormatDateTime(dateTime));
                case DateTimeOffset offset:
                    if (offset.Offset != TimeSpan.Zero) shadow = FormatOffset(offset.Offset);
                    return JsonValue.Create(FormatInstant(offset));
                case DateOnly date: return JsonValue.Create(date.ToString(_DateOnlyFormat, CultureInfo.InvariantCulture));
                case byte[] bytes: return JsonValue.Create(Convert.ToBase64String(bytes));
                default:
                    throw new InvalidOperationException("Values of type " + stored.GetType().Name + " cannot be stored in a Cosmos DB document (column kind " + kind + ").");
            }
        }

        /// <summary>
        /// Decodes a JSON value to a stored value of a column.
        /// </summary>
        /// <param name="node">JSON value; null for JSON null or a missing field.</param>
        /// <param name="shadow">Exact shadow text, or null.</param>
        /// <param name="stored">Stored type of the column. Must not be null.</param>
        /// <returns>The stored value, or null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the JSON value cannot be read as the stored type.</exception>
        public static object? Decode(JsonNode? node, string? shadow, Type stored)
        {
            if (node == null) return null;
            JsonElement element = ToElement(node);
            return Decode(element, shadow, stored);
        }

        /// <summary>
        /// Decodes a JSON element to a stored value of a column.
        /// </summary>
        /// <param name="element">JSON element.</param>
        /// <param name="shadow">Exact shadow text, or null.</param>
        /// <param name="stored">Stored type of the column. Must not be null.</param>
        /// <returns>The stored value, or null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the JSON value cannot be read as the stored type.</exception>
        public static object? Decode(JsonElement element, string? shadow, Type stored)
        {
            if (element.ValueKind == JsonValueKind.Null || element.ValueKind == JsonValueKind.Undefined) return null;
            try
            {
                return DecodeCore(element, shadow, stored);
            }
            catch (Exception e) when (e is FormatException || e is OverflowException || e is InvalidOperationException)
            {
                throw new InvalidOperationException("Cannot read the Cosmos DB value " + element.GetRawText() + " as " + stored.Name + ": " + e.Message, e);
            }
        }

        /// <summary>
        /// Converts a stored value to the value bound as a Cosmos DB query parameter, so it compares with stored values like
        /// the stored JSON value would.
        /// </summary>
        /// <param name="stored">Stored value. Must not be null.</param>
        /// <returns>The parameter value (string, long, ulong, double, decimal or bool).</returns>
        /// <exception cref="InvalidOperationException">Thrown when the value cannot be stored.</exception>
        public static object Parameter(object stored)
        {
            switch (stored)
            {
                case int i: return (long)i;
                case short s: return (long)s;
                case byte b: return (long)b;
                case sbyte sb: return (long)sb;
                case ushort us: return (long)us;
                case uint ui: return (long)ui;
                case TimeOnly time: return time.Ticks;
                case long l: return l;
                case ulong ul: return ul;
                case TimeSpan span: return span.Ticks;
                case decimal d: return d;
                case float f: return double.Parse(f.ToString("R", CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture);
                case double dbl: return dbl;
                case bool flag: return flag;
                case string text: return text;
                case char c: return c.ToString();
                case Guid guid: return guid.ToString("D");
                case DateTime dateTime: return FormatDateTime(dateTime);
                case DateTimeOffset offset: return FormatInstant(offset);
                case DateOnly date: return date.ToString(_DateOnlyFormat, CultureInfo.InvariantCulture);
                case byte[] bytes: return Convert.ToBase64String(bytes);
                default:
                    throw new InvalidOperationException("Values of type " + stored.GetType().Name + " cannot be used as Cosmos DB query parameters.");
            }
        }

        /// <summary>
        /// Returns the fixed-width wall-clock prefix of a DateTime (27 characters, years 0001 to 9999), which orders like
        /// <see cref="DateTime.Ticks"/>.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <returns>The prefix.</returns>
        public static string DateTimePrefix(DateTime value)
        {
            return value.ToString(_DateTimeFormat, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns whether a decimal survives a round trip through a double (the decimal's text parsed as a double, then
        /// formatted with the shortest round-trip text and parsed back as a decimal).
        /// </summary>
        /// <param name="value">Value.</param>
        /// <returns>True when a double holds the value exactly enough to round-trip.</returns>
        public static bool RoundTrips(decimal value)
        {
            double approximation = double.Parse(value.ToString(CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture);
            string back = approximation.ToString("R", CultureInfo.InvariantCulture);
            return decimal.TryParse(back, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal parsed) && parsed == value;
        }

        /// <summary>
        /// Returns whether a string contains a character at or above U+D800, where Cosmos DB's code point order and C#'s
        /// UTF-16 ordinal order can disagree.
        /// </summary>
        /// <param name="value">Value. Must not be null.</param>
        /// <returns>True when the orders may disagree for this value.</returns>
        public static bool HasHighCharacters(string value)
        {
            foreach (char c in value)
            {
                if (c >= '\uD800') return true;
            }

            return false;
        }

        /// <summary>
        /// Returns the element of a JSON node.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The element.</returns>
        public static JsonElement ToElement(JsonNode node)
        {
            if (node is JsonValue value && value.TryGetValue(out JsonElement element)) return element;
            using JsonDocument document = JsonDocument.Parse(node.ToJsonString());
            return document.RootElement.Clone();
        }

        #endregion

        #region Private-Methods

        private static object DecodeCore(JsonElement element, string? shadow, Type stored)
        {
            if (stored == typeof(string)) return element.ValueKind == JsonValueKind.String ? element.GetString()! : element.GetRawText();
            if (stored == typeof(char))
            {
                string text = element.GetString()!;
                return text.Length > 0 ? text[0] : '\0';
            }

            if (stored == typeof(bool)) return element.ValueKind == JsonValueKind.String ? bool.Parse(element.GetString()!) : element.GetBoolean();
            if (stored == typeof(Guid)) return Guid.Parse(element.GetString()!);
            if (stored == typeof(DateTime)) return ParseDateTime(element.GetString()!);
            if (stored == typeof(DateTimeOffset)) return ParseInstant(element.GetString()!, shadow);
            if (stored == typeof(DateOnly)) return DateOnly.ParseExact(element.GetString()!, _DateOnlyFormat, CultureInfo.InvariantCulture);
            if (stored == typeof(byte[])) return Convert.FromBase64String(element.GetString()!);
            if (stored == typeof(TimeOnly)) return new TimeOnly(ReadInt64(element, shadow));
            if (stored == typeof(TimeSpan)) return new TimeSpan(ReadInt64(element, shadow));
            if (stored == typeof(long)) return ReadInt64(element, shadow);
            if (stored == typeof(ulong))
            {
                if (shadow != null) return ulong.Parse(shadow, NumberStyles.Integer, CultureInfo.InvariantCulture);
                if (element.TryGetUInt64(out ulong exact)) return exact;
                return decimal.ToUInt64(decimal.Parse(element.GetRawText(), NumberStyles.Float, CultureInfo.InvariantCulture));
            }

            if (stored == typeof(decimal))
            {
                if (shadow != null) return decimal.Parse(shadow, NumberStyles.Float, CultureInfo.InvariantCulture);
                return decimal.Parse(Raw(element), NumberStyles.Float, CultureInfo.InvariantCulture);
            }

            if (stored == typeof(double))
            {
                if (element.ValueKind == JsonValueKind.String) return ParseNonFinite(element.GetString()!);
                return double.Parse(element.GetRawText(), NumberStyles.Float, CultureInfo.InvariantCulture);
            }

            if (stored == typeof(float))
            {
                if (element.ValueKind == JsonValueKind.String) return (float)ParseNonFinite(element.GetString()!);
                return float.Parse(element.GetRawText(), NumberStyles.Float, CultureInfo.InvariantCulture);
            }

            long integer = ReadInt64(element, shadow);
            return Convert.ChangeType(integer, stored, CultureInfo.InvariantCulture);
        }

        private static long ReadInt64(JsonElement element, string? shadow)
        {
            if (shadow != null) return long.Parse(shadow, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (element.ValueKind == JsonValueKind.String) return long.Parse(element.GetString()!, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (element.TryGetInt64(out long exact)) return exact;
            return decimal.ToInt64(decimal.Parse(element.GetRawText(), NumberStyles.Float, CultureInfo.InvariantCulture));
        }

        private static string Raw(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.String ? element.GetString()! : element.GetRawText();
        }

        private static double ParseNonFinite(string text)
        {
            switch (text)
            {
                case _NaN: return double.NaN;
                case _PositiveInfinity: return double.PositiveInfinity;
                case _NegativeInfinity: return double.NegativeInfinity;
                default: return double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            }
        }

        private static string FormatDateTime(DateTime value)
        {
            string prefix = DateTimePrefix(value);
            switch (value.Kind)
            {
                case DateTimeKind.Utc: return prefix + "Z";
                case DateTimeKind.Local: return prefix + FormatOffset(TimeZoneInfo.Local.GetUtcOffset(value));
                default: return prefix;
            }
        }

        private static DateTime ParseDateTime(string text)
        {
            if (text.Length >= _DateTimePrefixLength
                && DateTime.TryParseExact(text.Substring(0, _DateTimePrefixLength), _DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime wallClock))
            {
                string suffix = text.Substring(_DateTimePrefixLength);
                if (suffix.Length == 0) return DateTime.SpecifyKind(wallClock, DateTimeKind.Unspecified);
                if (suffix == "Z") return DateTime.SpecifyKind(wallClock, DateTimeKind.Utc);
                return DateTime.SpecifyKind(wallClock, DateTimeKind.Local);
            }

            return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        private static string FormatInstant(DateTimeOffset value)
        {
            return value.UtcDateTime.ToString(_DateTimeFormat, CultureInfo.InvariantCulture) + "Z";
        }

        private static DateTimeOffset ParseInstant(string text, string? shadow)
        {
            DateTimeOffset utc;
            if (text.Length == _DateTimePrefixLength + 1 && text[_DateTimePrefixLength] == 'Z'
                && DateTime.TryParseExact(text.Substring(0, _DateTimePrefixLength), _DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime instant))
            {
                utc = new DateTimeOffset(DateTime.SpecifyKind(instant, DateTimeKind.Utc));
            }
            else
            {
                utc = DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
            }

            if (shadow == null) return utc;
            return utc.ToOffset(ParseOffset(shadow));
        }

        private static string FormatOffset(TimeSpan offset)
        {
            string sign = offset < TimeSpan.Zero ? "-" : "+";
            TimeSpan magnitude = offset.Duration();
            return sign + magnitude.Hours.ToString("00", CultureInfo.InvariantCulture) + ":" + magnitude.Minutes.ToString("00", CultureInfo.InvariantCulture);
        }

        private static TimeSpan ParseOffset(string text)
        {
            bool negative = text.StartsWith("-", StringComparison.Ordinal);
            string body = text.TrimStart('+', '-');
            string[] parts = body.Split(':');
            TimeSpan magnitude = new TimeSpan(int.Parse(parts[0], CultureInfo.InvariantCulture), parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 0, 0);
            return negative ? -magnitude : magnitude;
        }

        #endregion
    }
}
