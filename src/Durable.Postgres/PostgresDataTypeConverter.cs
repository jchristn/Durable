namespace Durable.Postgres
{
    using System;
    using System.Text.Json;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// PostgreSQL value conversion. Npgsql handles most types natively; this converter makes <see cref="DateTime"/> values
    /// unspecified-kind (for <c>timestamp</c> columns) and <see cref="DateTimeOffset"/> values UTC (for <c>timestamptz</c>),
    /// matching Npgsql 6+ rules, and widens unsigned integers to signed types PostgreSQL supports.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class PostgresDataTypeConverter : DataTypeConverter
    {
        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public PostgresDataTypeConverter(JsonSerializerOptions? jsonOptions = null) : base(jsonOptions)
        {
        }

        /// <inheritdoc />
        protected override object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            switch (value)
            {
                case DateTime dateTime:
                    return dateTime.Kind == DateTimeKind.Unspecified ? dateTime : DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);
                case DateTimeOffset dateTimeOffset:
                    return dateTimeOffset.ToUniversalTime();
                case uint unsignedInt:
                    return (long)unsignedInt;
                case ushort unsignedShort:
                    return (int)unsignedShort;
                case ulong unsignedLong:
                    return (decimal)unsignedLong;
                case sbyte signedByte:
                    return (short)signedByte;
                case byte unsignedByte:
                    return (short)unsignedByte;
                case char c:
                    return c.ToString();
                default:
                    return value;
            }
        }
    }
}
