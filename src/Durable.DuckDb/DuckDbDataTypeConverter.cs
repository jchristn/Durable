namespace Durable.DuckDb
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Numerics;
    using System.Text.Json;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// DuckDB value conversion. DuckDB.NET binds and reads most CLR types natively (including unsigned integers, GUIDs as
    /// UUID, DateOnly/TimeOnly, TimeSpan as INTERVAL and DateTimeOffset as TIMESTAMPTZ). This converter covers what the
    /// driver returns in other shapes: BLOB columns are read as a <see cref="Stream"/> (copied to a byte array),
    /// HUGEINT/UHUGEINT values (for example SUM over integer columns) are read as <see cref="BigInteger"/> and narrowed to
    /// the target numeric type, and <see cref="DateTime"/> values are written with unspecified kind for TIMESTAMP columns.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public class DuckDbDataTypeConverter : DataTypeConverter
    {
        /// <summary>
        /// Instantiates the converter.
        /// </summary>
        /// <param name="jsonOptions">JSON options for JSON columns; null uses the defaults.</param>
        public DuckDbDataTypeConverter(JsonSerializerOptions? jsonOptions = null) : base(jsonOptions)
        {
        }

        /// <inheritdoc />
        protected override object ToDatabaseCore(object value, Type type, ColumnMetadata? column)
        {
            switch (value)
            {
                case DateTime dateTime:
                    return dateTime.Kind == DateTimeKind.Unspecified ? dateTime : DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);
                case char c:
                    return c.ToString();
                default:
                    return value;
            }
        }

        /// <inheritdoc />
        protected override object? FromDatabaseCore(object value, Type valueType, Type targetType, ColumnMetadata? column)
        {
            if (value is Stream stream)
            {
                byte[] bytes = ReadAll(stream);
                if (targetType == typeof(byte[])) return bytes;
                return base.FromDatabaseCore(bytes, typeof(byte[]), targetType, column);
            }

            if (value is BigInteger big)
            {
                if (targetType == typeof(BigInteger)) return big;
                if (targetType == typeof(long)) return (long)big;
                if (targetType == typeof(int)) return (int)big;
                if (targetType == typeof(short)) return (short)big;
                if (targetType == typeof(sbyte)) return (sbyte)big;
                if (targetType == typeof(byte)) return (byte)big;
                if (targetType == typeof(ushort)) return (ushort)big;
                if (targetType == typeof(uint)) return (uint)big;
                if (targetType == typeof(ulong)) return (ulong)big;
                if (targetType == typeof(decimal)) return (decimal)big;
                if (targetType == typeof(double)) return (double)big;
                if (targetType == typeof(float)) return (float)big;
                if (targetType == typeof(bool)) return !big.IsZero;
                if (targetType == typeof(string)) return big.ToString(CultureInfo.InvariantCulture);
                if (targetType == typeof(object)) return big;
                return Convert.ChangeType((decimal)big, targetType, CultureInfo.InvariantCulture);
            }

            return base.FromDatabaseCore(value, valueType, targetType, column);
        }

        private static byte[] ReadAll(Stream stream)
        {
            if (stream is MemoryStream memory) return memory.ToArray();
            using MemoryStream copy = new MemoryStream();
            stream.CopyTo(copy);
            return copy.ToArray();
        }
    }
}
