namespace Durable.MySql
{
    using System;
    using System.Text.Json;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// MySQL value conversion. MySqlConnector handles most types natively; <see cref="DateTimeOffset"/> values are stored
    /// as UTC <c>DATETIME</c> values because MySQL has no offset-aware type, GUIDs are stored as 36-character strings,
    /// and <see cref="TimeSpan"/> values are stored as BIGINT ticks so durations beyond MySQL's TIME range round-trip.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class MySqlDataTypeConverter : DataTypeConverter
    {
        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public MySqlDataTypeConverter(JsonSerializerOptions? jsonOptions = null) : base(jsonOptions)
        {
        }

        /// <inheritdoc />
        protected override object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            switch (value)
            {
                case DateTimeOffset dateTimeOffset:
                    return dateTimeOffset.UtcDateTime;
                case TimeSpan timeSpan:
                    return timeSpan.Ticks;
                case Guid guid:
                    return guid.ToString("D");
                case char c:
                    return c.ToString();
                default:
                    return value;
            }
        }
    }
}
