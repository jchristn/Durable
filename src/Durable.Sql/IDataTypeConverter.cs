namespace Durable.Sql
{
    using System;
    using Durable;

    /// <summary>
    /// Converts values between CLR types and the representation a specific database expects.
    /// Column-level <see cref="IValueConverter"/> instances run before (on write) and after (on read) this converter.
    /// Thread safety: implementations must be thread-safe.
    /// </summary>
    public interface IDataTypeConverter
    {
        /// <summary>
        /// Converts a CLR value to a database parameter value.
        /// </summary>
        /// <param name="value">CLR value; may be null.</param>
        /// <param name="column">Target column when known; may be null.</param>
        /// <returns>The database value; <see cref="DBNull.Value"/> for null.</returns>
        object ConvertToDatabase(object? value, ColumnMetadata? column = null);

        /// <summary>
        /// Converts a database value to a CLR type.
        /// </summary>
        /// <param name="value">Database value; may be null or <see cref="DBNull"/>.</param>
        /// <param name="targetType">Target CLR type. Must not be null.</param>
        /// <param name="column">Source column when known; may be null.</param>
        /// <returns>The converted value; null (or the default of a non-nullable value type) for database nulls.</returns>
        /// <exception cref="ArgumentNullException">Thrown when targetType is null.</exception>
        /// <exception cref="InvalidCastException">Thrown when the value cannot be converted.</exception>
        object? ConvertFromDatabase(object? value, Type targetType, ColumnMetadata? column = null);
    }
}
