namespace Durable.InMemory
{
    using System;
    using System.Globalization;
    using System.Text.Json;
    using Durable;

    /// <summary>
    /// Converts between entity property values and the values an <see cref="InMemoryBackend"/> stores, the way a database
    /// driver would: per-property <see cref="IValueConverter"/>s store their provider value; JSON columns store serialized
    /// text; enums store their name (or their numeric value with <see cref="Flags.Integer"/>); byte arrays are copied;
    /// numbers are stored in the column's CLR type. Decimals keep full precision (no column scale is applied) and
    /// <see cref="DateTime"/> values keep their <see cref="DateTimeKind"/>.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class InMemoryValueConverter
    {
        #region Public-Members

        /// <summary>
        /// Gets the JSON options used for JSON columns. Never null.
        /// </summary>
        public JsonSerializerOptions JsonOptions { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when jsonOptions is null.</exception>
        public InMemoryValueConverter(JsonSerializerOptions jsonOptions)
        {
            JsonOptions = jsonOptions ?? throw new ArgumentNullException(nameof(jsonOptions));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Converts a property (model) value to its stored value. Integral values for enum columns are converted to the
        /// enum first, as SQL parameters are.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="value">Model value; may be null.</param>
        /// <returns>The stored value, or null.</returns>
        public object? ToStored(ColumnMetadata column, object? value)
        {
            if (value == null) return null;

            if (column.Converter != null)
            {
                if (!column.Converter.ModelType.IsInstanceOfType(value) && column.Converter.ProviderType.IsInstanceOfType(value)) return value;
                return column.Converter.ToProvider(value);
            }

            if (column.IsJson)
            {
                if (value is string text && (column.ClrType == typeof(string) || !column.ClrType.IsInstanceOfType(value))) return text;
                return JsonSerializer.Serialize(value, value.GetType(), JsonOptions);
            }

            if (column.IsEnum)
            {
                if (value is string name) return column.EnumAsString ? name : Convert.ToInt64(Enum.Parse(column.ClrType, name, true), CultureInfo.InvariantCulture);
                object enumValue = value is Enum ? value : Enum.ToObject(column.ClrType, value);
                return column.EnumAsString ? enumValue.ToString() : Convert.ToInt64(enumValue, CultureInfo.InvariantCulture);
            }

            if (value is byte[] bytes) return bytes.Clone();
            return Coerce(value, column.ClrType);
        }

        /// <summary>
        /// Converts a value computed by a query expression (already in the stored domain, or a plain CLR value) to the
        /// stored representation of a column, applying a converter only to values of the converter's model type.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="value">Computed value; may be null.</param>
        /// <returns>The stored value, or null.</returns>
        public object? ComputedToStored(ColumnMetadata column, object? value)
        {
            if (value == null) return null;
            if (column.Converter != null)
            {
                if (column.Converter.ModelType != column.Converter.ProviderType && column.Converter.ModelType.IsInstanceOfType(value))
                    return column.Converter.ToProvider(value);
                return value;
            }

            return ToStored(column, value);
        }

        /// <summary>
        /// Converts a stored value back to a property value.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="stored">Stored value; may be null.</param>
        /// <returns>The property value, or null.</returns>
        public object? FromStored(ColumnMetadata column, object? stored)
        {
            if (stored == null) return null;
            if (column.Converter != null) return column.Converter.FromProvider(stored);

            if (column.IsJson)
            {
                if (column.ClrType == typeof(string)) return stored as string ?? Convert.ToString(stored, CultureInfo.InvariantCulture);
                string json = stored as string ?? Convert.ToString(stored, CultureInfo.InvariantCulture) ?? string.Empty;
                return json.Length == 0 ? null : JsonSerializer.Deserialize(json, column.ClrType, JsonOptions);
            }

            if (column.IsEnum)
            {
                if (stored is string name) return Enum.Parse(column.ClrType, name, true);
                return Enum.ToObject(column.ClrType, Convert.ChangeType(stored, Enum.GetUnderlyingType(column.ClrType), CultureInfo.InvariantCulture)!);
            }

            if (stored is byte[] bytes) return bytes.Clone();
            return Coerce(stored, column.ClrType);
        }

        #endregion

        #region Private-Methods

        private static object Coerce(object value, Type target)
        {
            Type type = value.GetType();
            if (type == target || target.IsAssignableFrom(type)) return value;
            if (IsNumeric(type) && IsNumeric(target)) return Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
            if (target == typeof(string) && value is char c) return c.ToString();
            if (target == typeof(char) && value is string s) return s.Length > 0 ? s[0] : '\0';
            if (target == typeof(DateTimeOffset) && value is DateTime dateTime) return new DateTimeOffset(dateTime);
            if (target == typeof(DateTime) && value is DateTimeOffset offset) return offset.UtcDateTime;
            if (target == typeof(DateOnly) && value is DateTime date) return DateOnly.FromDateTime(date);
            if (target == typeof(bool) && IsNumeric(type)) return Convert.ToInt64(value, CultureInfo.InvariantCulture) != 0;
            return value;
        }

        private static bool IsNumeric(Type type)
        {
            return type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)
                || type == typeof(uint) || type == typeof(ulong) || type == typeof(ushort) || type == typeof(sbyte)
                || type == typeof(decimal) || type == typeof(double) || type == typeof(float);
        }

        #endregion
    }
}
