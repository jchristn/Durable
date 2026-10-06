namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees against LiteGraph rows with the C# semantics of
    /// <see cref="QueryEvaluator{TRow}"/>. Values are compared in their stored form (enum names, converter provider values,
    /// JSON text): parameter values are converted with <see cref="LiteGraphValueConverter.ToStored"/>, exactly as a
    /// database compares column values with converted parameters. Related rows (navigation members, collections,
    /// many-to-many junctions) come from a <see cref="LiteGraphRowSet"/> loaded for the operation, through hash indexes.
    /// Thread safety: not thread-safe; create one per operation.
    /// </summary>
    internal sealed class LiteGraphQueryEvaluator : QueryEvaluator<LiteGraphRow>
    {
        private readonly LiteGraphValueConverter _Values;
        private readonly LiteGraphRowSet _Related;

        /// <summary>
        /// Instantiates an evaluator.
        /// </summary>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <param name="related">Rows of the related entity types the evaluated trees reference. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public LiteGraphQueryEvaluator(LiteGraphValueConverter values, LiteGraphRowSet related)
        {
            _Values = values ?? throw new ArgumentNullException(nameof(values));
            _Related = related ?? throw new ArgumentNullException(nameof(related));
        }

        /// <inheritdoc />
        protected override object? GetValue(EntityMetadata metadata, LiteGraphRow row, ColumnMetadata column)
        {
            return row.Values[LiteGraphTableSchema.For(metadata).Ordinal(column)];
        }

        /// <inheritdoc />
        protected override IEnumerable<LiteGraphRow> FindRows(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            return _Related.Find(metadata, column, key);
        }

        /// <inheritdoc />
        protected override object? NormalizeValue(ColumnMetadata column, object? value)
        {
            return _Values.ToStored(column, value);
        }
    }
}
