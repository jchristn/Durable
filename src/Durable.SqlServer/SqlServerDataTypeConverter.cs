namespace Durable.SqlServer
{
    using System;
    using System.Text.Json;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// SQL Server value conversion. SqlClient handles most types natively; unsigned integers are widened to the next
    /// signed type SQL Server supports, and <see cref="TimeSpan"/> values are stored as BIGINT ticks because the TIME type
    /// cannot hold durations of 24 hours or more.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class SqlServerDataTypeConverter : DataTypeConverter
    {
        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public SqlServerDataTypeConverter(JsonSerializerOptions? jsonOptions = null) : base(jsonOptions)
        {
        }

        /// <inheritdoc />
        protected override object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            switch (value)
            {
                case TimeSpan timeSpan:
                    return timeSpan.Ticks;
                case uint unsignedInt:
                    return (long)unsignedInt;
                case ushort unsignedShort:
                    return (int)unsignedShort;
                case ulong unsignedLong:
                    return (decimal)unsignedLong;
                case sbyte signedByte:
                    return (short)signedByte;
                case char c:
                    return c.ToString();
                default:
                    return value;
            }
        }
    }
}
