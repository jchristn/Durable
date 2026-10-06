namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// A minimal <see cref="QueryEvaluator{TRow}"/> over dictionary rows (property name to value), proving the evaluator is
    /// reusable outside Durable.InMemory. Enum columns stored as strings hold the enum name, so parameter values are
    /// normalized to names.
    /// Thread safety: not thread-safe; create one per evaluation.
    /// </summary>
    public sealed class DictionaryQueryEvaluator : QueryEvaluator<Dictionary<string, object?>>
    {
        private readonly Dictionary<Type, List<Dictionary<string, object?>>> _Tables;

        /// <summary>
        /// Instantiates an evaluator over tables of dictionary rows.
        /// </summary>
        /// <param name="tables">Rows per entity type. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when tables is null.</exception>
        public DictionaryQueryEvaluator(Dictionary<Type, List<Dictionary<string, object?>>> tables)
        {
            _Tables = tables ?? throw new ArgumentNullException(nameof(tables));
        }

        /// <inheritdoc />
        protected override object? GetValue(EntityMetadata metadata, Dictionary<string, object?> row, ColumnMetadata column)
        {
            return row.TryGetValue(column.Property.Name, out object? value) ? value : null;
        }

        /// <inheritdoc />
        protected override IEnumerable<Dictionary<string, object?>> FindRows(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            if (!_Tables.TryGetValue(metadata.EntityType, out List<Dictionary<string, object?>>? rows)) return Array.Empty<Dictionary<string, object?>>();
            return rows.Where(row => QueryValueComparer.Ordinal.Equals(GetValue(metadata, row, column), key));
        }

        /// <inheritdoc />
        protected override object? NormalizeValue(ColumnMetadata column, object? value)
        {
            if (value != null && column.IsEnum && column.EnumAsString) return value.ToString();
            return value;
        }
    }
}
