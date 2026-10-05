namespace Durable.Sql
{
    using System;
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Text.Json;
    using Durable;
    using Durable.Sql.Helpers;

    /// <summary>
    /// Default conversion between CLR values and ADO.NET parameter/reader values. Providers whose drivers handle a type
    /// natively pass it through; providers with weaker type systems (SQLite) override the virtual hooks.
    /// Rules: enums are stored by name unless the column has <see cref="Flags.Integer"/>; collections and complex objects
    /// (and <see cref="Flags.Json"/> columns) are stored as JSON text; column <see cref="IValueConverter"/>s run first on write
    /// and last on read. All formatting is culture-invariant.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class DataTypeConverter : IDataTypeConverter
    {
        #region Public-Members

        /// <summary>
        /// Gets the JSON options used for JSON columns. Default: camelCase property names, compact output.
        /// </summary>
        public JsonSerializerOptions JsonOptions { get; }

        #endregion

        #region Private-Members

        private static readonly JsonSerializerOptions _DefaultJsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        private static readonly ConcurrentDictionary<Type, object> _Defaults = new ConcurrentDictionary<Type, object>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public DataTypeConverter(JsonSerializerOptions? jsonOptions = null)
        {
            JsonOptions = jsonOptions ?? _DefaultJsonOptions;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public object ConvertToDatabase(object? value, ColumnMetadata? column = null)
        {
            if (value == null || value == DBNull.Value) return DBNull.Value;

            if (column?.Converter != null)
            {
                object? provided = column.Converter.ToProvider(value);
                if (provided == null) return DBNull.Value;
                return ToDatabaseCore(provided, provided.GetType(), null);
            }

            Type type = value.GetType();

            if (type.IsEnum)
            {
                if (column != null && !column.EnumAsString)
                    return Convert.ChangeType(value, Enum.GetUnderlyingType(type), CultureInfo.InvariantCulture);
                return value.ToString()!;
            }

            if ((column != null && column.IsJson) || (column == null && !EntityMetadata.IsScalarType(type)))
            {
                if (value is string alreadyJson && column != null && (column.Flags & Flags.Json) == Flags.Json && column.ClrType == typeof(string))
                    return alreadyJson;
                return JsonSerializer.Serialize(value, type, JsonOptions);
            }

            return ToDatabaseCore(value, type, column);
        }

        /// <inheritdoc />
        public object? ConvertFromDatabase(object? value, Type targetType, ColumnMetadata? column = null)
        {
            ArgumentNullException.ThrowIfNull(targetType);

            if (value == null || value == DBNull.Value)
                return DefaultOf(targetType);

            if (column?.Converter != null)
            {
                object? provided = ConvertFromDatabase(value, column.Converter.ProviderType, null);
                return column.Converter.FromProvider(provided);
            }

            Type underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
            Type valueType = value.GetType();
            if (underlying == valueType || (underlying != typeof(object) && underlying.IsAssignableFrom(valueType) && !underlying.IsEnum))
            {
                if (!(column != null && column.IsJson && value is string && underlying != typeof(string)))
                    return value;
            }

            if (underlying.IsEnum)
            {
                if (value is string enumName)
                    return Enum.Parse(underlying, enumName, true);
                return Enum.ToObject(underlying, Convert.ChangeType(value, Enum.GetUnderlyingType(underlying), CultureInfo.InvariantCulture)!);
            }

            if ((column != null && column.IsJson) || !EntityMetadata.IsScalarType(underlying))
            {
                if (value is string json)
                    return string.IsNullOrEmpty(json) ? DefaultOf(targetType) : JsonSerializer.Deserialize(json, underlying, JsonOptions);
                if (value is byte[] jsonBytes)
                    return JsonSerializer.Deserialize(jsonBytes, underlying, JsonOptions);
                return JsonSerializer.Deserialize(value.ToString()!, underlying, JsonOptions);
            }

            return FromDatabaseCore(value, valueType, underlying, column);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Converts a scalar CLR value (enums and JSON already handled) to a database value. Override for databases
        /// lacking native support for a type. The default passes values through, converting <see cref="char"/> to string.
        /// </summary>
        /// <param name="value">Non-null value.</param>
        /// <param name="type">Runtime type of the value.</param>
        /// <param name="column">Target column; may be null.</param>
        /// <returns>The database value.</returns>
        protected virtual object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            if (type == typeof(char)) return value.ToString()!;
            return value;
        }

        /// <summary>
        /// Converts a non-null database value to a scalar CLR type (enums and JSON already handled).
        /// </summary>
        /// <param name="value">Non-null database value.</param>
        /// <param name="valueType">Runtime type of the value.</param>
        /// <param name="targetType">Target type with any nullable wrapper removed.</param>
        /// <param name="column">Source column; may be null.</param>
        /// <returns>The converted value.</returns>
        /// <exception cref="InvalidCastException">Thrown when the value cannot be converted.</exception>
        protected virtual object? FromDatabaseCore(object value, Type valueType, Type targetType, ColumnMetadata? column)
        {
            if (targetType == typeof(bool)) return ToBoolean(value);

            if (targetType == typeof(Guid))
            {
                if (value is string guidText) return Guid.Parse(guidText);
                if (value is byte[] guidBytes) return guidBytes.Length == 16 ? new Guid(guidBytes) : Guid.Parse(System.Text.Encoding.ASCII.GetString(guidBytes));
                return Guid.Parse(value.ToString()!);
            }

            if (targetType == typeof(DateTime))
            {
                if (value is string dateText) return DateTimeParser.ParseString(dateText);
                if (value is DateTimeOffset dto) return dto.UtcDateTime;
                if (value is DateOnly dateOnly) return dateOnly.ToDateTime(TimeOnly.MinValue);
                return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
            }

            if (targetType == typeof(DateTimeOffset))
            {
                if (value is string dtoText) return DateTimeOffsetParser.ParseString(dtoText);
                if (value is DateTime dateTime) return dateTime.Kind == DateTimeKind.Unspecified ? new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)) : new DateTimeOffset(dateTime);
                return DateTimeOffsetParser.ParseString(value.ToString()!);
            }

            if (targetType == typeof(DateOnly))
            {
                if (value is DateTime dateTime) return DateOnly.FromDateTime(dateTime);
                if (value is string dateText) return DateOnly.FromDateTime(DateTimeParser.ParseString(dateText));
                return DateOnly.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            if (targetType == typeof(TimeOnly))
            {
                if (value is TimeSpan timeSpan) return TimeOnly.FromTimeSpan(timeSpan);
                if (value is DateTime dateTime) return TimeOnly.FromDateTime(dateTime);
                return TimeOnly.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            if (targetType == typeof(TimeSpan))
            {
                if (value is string spanText) return TimeSpan.Parse(spanText, CultureInfo.InvariantCulture);
                if (value is TimeOnly timeOnly) return timeOnly.ToTimeSpan();
                if (value is long ticks) return TimeSpan.FromTicks(ticks);
                if (value is DateTime dateTime) return dateTime.TimeOfDay;
                return TimeSpan.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            if (targetType == typeof(char))
            {
                string text = value.ToString()!;
                return text.Length > 0 ? text[0] : '\0';
            }

            if (targetType == typeof(string))
            {
                if (value is byte[] bytes) return System.Text.Encoding.UTF8.GetString(bytes);
                if (value is IFormattable formattable) return formattable.ToString(null, CultureInfo.InvariantCulture);
                return value.ToString();
            }

            if (targetType == typeof(byte[]))
            {
                if (value is string base64) return Convert.FromBase64String(base64);
                throw new InvalidCastException("Cannot convert " + valueType.Name + " to byte[].");
            }

            return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
        }


        private static object? DefaultOf(Type targetType)
        {
            if (!targetType.IsValueType || Nullable.GetUnderlyingType(targetType) != null) return null;
            return _Defaults.GetOrAdd(targetType, t => Activator.CreateInstance(t)!);
        }

        private static bool ToBoolean(object value)
        {
            switch (value)
            {
                case bool b: return b;
                case long l: return l != 0;
                case int i: return i != 0;
                case short s: return s != 0;
                case byte by: return by != 0;
                case sbyte sb: return sb != 0;
                case ulong ul: return ul != 0;
                case uint ui: return ui != 0;
                case ushort us: return us != 0;
                case decimal d: return d != 0;
                case double db: return db != 0;
                case float f: return f != 0;
                case string text:
                    {
                        string trimmed = text.Trim();
                        if (trimmed.Length == 0 || trimmed == "0") return false;
                        if (string.Equals(trimmed, "false", StringComparison.OrdinalIgnoreCase)) return false;
                        return true;
                    }
                default:
                    return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
            }
        }

        #endregion
    }
}
