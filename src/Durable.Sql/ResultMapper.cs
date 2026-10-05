namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using Durable;

    /// <summary>
    /// Creates row mappers for arbitrary result types: scalar types read the first column; other types are materialized
    /// by column name with <see cref="RowMaterializer"/>.
    /// Thread safety: returned mappers are not thread-safe (they cache the materializer on first use); create one per command.
    /// </summary>
    public static class ResultMapper
    {
        /// <summary>
        /// Creates a mapper for <typeparamref name="TResult"/>.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <returns>The mapper.</returns>
        /// <exception cref="ArgumentNullException">Thrown when converter is null.</exception>
        public static Func<DbDataReader, TResult> Create<TResult>(IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(converter);
            Type type = typeof(TResult);
            if (EntityMetadata.IsScalarType(type) || type == typeof(object))
            {
                return reader =>
                {
                    if (reader.IsDBNull(0)) return default!;
                    object raw = reader.GetValue(0);
                    if (type == typeof(object)) return (TResult)raw;
                    return (TResult)converter.ConvertFromDatabase(raw, type)!;
                };
            }

            return RowMaterializer.CreateMapper<TResult>(EntityMetadata.For(type), converter);
        }
    }
}
