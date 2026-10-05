namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using Durable;

    /// <summary>
    /// Per-command row mapper created by <see cref="RowMaterializer.CreateMapper{T}"/>: resolves the materializer and its
    /// compiled typed reader on the first row, then maps every row with a single delegate call.
    /// Thread safety: not thread-safe; use one instance per command.
    /// </summary>
    /// <typeparam name="T">Result type.</typeparam>
    internal sealed class TypedRowMapper<T>
    {
        private readonly EntityMetadata _Metadata;
        private readonly IDataTypeConverter _Converter;
        private readonly bool _Inline;
        private RowMaterializer? _Materializer;
        private Func<DbDataReader, IDataTypeConverter, T>? _Reader;

        internal TypedRowMapper(EntityMetadata metadata, IDataTypeConverter converter, bool inline)
        {
            _Metadata = metadata;
            _Converter = converter;
            _Inline = inline;
        }

        internal T Map(DbDataReader reader)
        {
            RowMaterializer? materializer = _Materializer;
            if (materializer == null)
            {
                materializer = RowMaterializer.For(_Metadata, reader);
                _Reader = typeof(T) == _Metadata.EntityType ? materializer.GetTypedReader<T>(_Inline) : null;
                _Materializer = materializer;
            }

            if (_Reader == null) return (T)materializer.Materialize(reader, _Converter, _Inline);
            return materializer.Read(reader, _Converter, _Reader);
        }
    }
}
