namespace Durable.Sqlite
{
    using System;
    using System.Globalization;
    using System.Text.Json;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// SQLite value conversion. SQLite has no native date, time, GUID or decimal types, so these are stored as
    /// culture-invariant text (dates as "yyyy-MM-dd HH:mm:ss.fffffff", which sorts and compares correctly) and decimals as REAL.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class SqliteDataTypeConverter : DataTypeConverter
    {
        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public SqliteDataTypeConverter(JsonSerializerOptions? jsonOptions = null) : base(jsonOptions)
        {
        }

        /// <inheritdoc />
        protected override object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            switch (value)
            {
                case DateTime dateTime:
                    return dateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                case DateTimeOffset dateTimeOffset:
                    return dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture);
                case DateOnly dateOnly:
                    return dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                case TimeOnly timeOnly:
                    return timeOnly.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                case TimeSpan timeSpan:
                    return timeSpan.ToString("c", CultureInfo.InvariantCulture);
                case Guid guid:
                    return guid.ToString("D");
                case decimal number:
                    return (double)number;
                case bool flag:
                    return flag ? 1L : 0L;
                case char c:
                    return c.ToString();
                default:
                    return value;
            }
        }
    }
}
