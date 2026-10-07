namespace Durable.Oracle
{
    using System;
    using System.Text.Json;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// Oracle value conversion. Booleans are stored as NUMBER(1) (1/0) so the same schema works on Oracle 19c and later,
    /// <see cref="Guid"/> values as RAW(16) in RFC 4122 (big-endian) byte order so the stored bytes read like the textual
    /// form, <see cref="TimeOnly"/> as an INTERVAL DAY TO SECOND, <see cref="DateOnly"/> as DATE, and unsigned integers are
    /// widened to the next signed type. <see cref="TimeSpan"/>, <see cref="DateTime"/> and <see cref="DateTimeOffset"/> pass
    /// through to ODP.NET (INTERVAL DAY TO SECOND, TIMESTAMP, TIMESTAMP WITH TIME ZONE).
    /// Oracle stores an empty string as NULL; see <see cref="OracleDialect.TreatsEmptyStringAsNull"/>.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class OracleDataTypeConverter : DataTypeConverter
    {
        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public OracleDataTypeConverter(JsonSerializerOptions? jsonOptions = null) : base(jsonOptions)
        {
        }

        /// <inheritdoc />
        protected override object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            switch (value)
            {
                case bool flag:
                    return flag ? (short)1 : (short)0;
                case Guid guid:
                    return guid.ToByteArray(true);
                case TimeOnly time:
                    return time.ToTimeSpan();
                case DateOnly date:
                    return date.ToDateTime(TimeOnly.MinValue);
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

        /// <inheritdoc />
        protected override object? FromDatabaseCore(object value, Type valueType, Type targetType, ColumnMetadata? column)
        {
            if (targetType == typeof(Guid) && value is byte[] bytes && bytes.Length == 16) return new Guid(bytes, true);
            if (targetType == typeof(TimeSpan) && value is decimal ticks) return TimeSpan.FromTicks((long)ticks);
            return base.FromDatabaseCore(value, valueType, targetType, column);
        }
    }
}
