namespace Durable.Query
{
    using System;
    using System.Globalization;

    /// <summary>
    /// Converts aggregate and scalar results returned by a backend (or computed on the client) to the CLR type a caller
    /// asked for: numeric widening and narrowing, enum names and numbers, and nullable wrappers.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class ScalarConversion
    {
        #region Public-Methods

        /// <summary>
        /// Converts a value to <typeparamref name="TResult"/>.
        /// </summary>
        /// <typeparam name="TResult">Target type.</typeparam>
        /// <param name="value">Value; null returns the default of <typeparamref name="TResult"/>.</param>
        /// <returns>The converted value.</returns>
        /// <exception cref="InvalidCastException">Thrown when the value cannot be converted.</exception>
        public static TResult To<TResult>(object? value)
        {
            object? converted = To(value, typeof(TResult));
            return converted == null ? default! : (TResult)converted;
        }

        /// <summary>
        /// Converts a value to a target type.
        /// </summary>
        /// <param name="value">Value; null returns null.</param>
        /// <param name="targetType">Target type. Must not be null.</param>
        /// <returns>The converted value, or null.</returns>
        /// <exception cref="InvalidCastException">Thrown when the value cannot be converted.</exception>
        public static object? To(object? value, Type targetType)
        {
            ArgumentNullException.ThrowIfNull(targetType);
            if (value == null) return null;
            if (targetType.IsInstanceOfType(value)) return value;

            Type underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (underlying.IsInstanceOfType(value)) return value;

            if (underlying.IsEnum)
            {
                if (value is string name) return Enum.Parse(underlying, name, true);
                return Enum.ToObject(underlying, Convert.ChangeType(value, Enum.GetUnderlyingType(underlying), CultureInfo.InvariantCulture)!);
            }

            if (value is Enum && IsNumeric(underlying))
                return Convert.ChangeType(Convert.ToInt64(value, CultureInfo.InvariantCulture), underlying, CultureInfo.InvariantCulture);
            if (underlying == typeof(string)) return Convert.ToString(value, CultureInfo.InvariantCulture);
            if (underlying == typeof(DateTimeOffset) && value is DateTime dateTime) return new DateTimeOffset(dateTime);
            if (underlying == typeof(DateTime) && value is DateTimeOffset offset) return offset.UtcDateTime;
            if (underlying == typeof(DateOnly) && value is DateTime date) return DateOnly.FromDateTime(date);
            if (value is IConvertible) return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
            throw new InvalidCastException("Cannot convert " + value.GetType().Name + " to " + targetType.Name + ".");
        }

        /// <summary>
        /// Determines whether a type is a built-in numeric type.
        /// </summary>
        /// <param name="type">Type. Must not be null.</param>
        /// <returns>True for integral and floating-point types and decimal.</returns>
        public static bool IsNumeric(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            Type t = Nullable.GetUnderlyingType(type) ?? type;
            return t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)
                || t == typeof(uint) || t == typeof(ulong) || t == typeof(ushort) || t == typeof(sbyte)
                || t == typeof(decimal) || t == typeof(double) || t == typeof(float);
        }

        #endregion
    }
}
