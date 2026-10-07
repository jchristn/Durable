namespace Durable.CosmosDb
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees client-side against Cosmos DB documents with the C# semantics of
    /// <see cref="QueryEvaluator{TRow}"/>: everything a <see cref="CosmosDbBackend"/> does not push down (residual
    /// predicates, navigations, collection predicates, computed values, ordering, paging and aggregates). Values are
    /// compared in their stored form (enum names, converter provider values, JSON text): properties are decoded with
    /// <see cref="CosmosDbJsonCodec"/> and parameter values converted with <see cref="CosmosDbValueConverter.ToStored"/>.
    /// Related documents are looked up through the backend (a point read by key or a query by property) and cached for the
    /// evaluator's lifetime; the lookup blocks the calling thread because the evaluator is synchronous.
    /// Thread safety: not thread-safe; create one per operation.
    /// </summary>
    internal sealed class CosmosDbQueryEvaluator : QueryEvaluator<CosmosDbDocument>
    {
        private readonly CosmosDbValueConverter _Values;
        private readonly Func<EntityMetadata, ColumnMetadata, object, IReadOnlyList<CosmosDbDocument>> _FindRows;
        private readonly Dictionary<ColumnMetadata, Dictionary<object, IReadOnlyList<CosmosDbDocument>>> _Related = new Dictionary<ColumnMetadata, Dictionary<object, IReadOnlyList<CosmosDbDocument>>>(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Instantiates an evaluator.
        /// </summary>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <param name="findRows">Looks up the documents of an entity whose column equals a stored value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public CosmosDbQueryEvaluator(CosmosDbValueConverter values, Func<EntityMetadata, ColumnMetadata, object, IReadOnlyList<CosmosDbDocument>> findRows)
        {
            _Values = values ?? throw new ArgumentNullException(nameof(values));
            _FindRows = findRows ?? throw new ArgumentNullException(nameof(findRows));
        }

        /// <inheritdoc />
        protected override object? GetValue(EntityMetadata metadata, CosmosDbDocument row, ColumnMetadata column)
        {
            return row.Get(column);
        }

        /// <inheritdoc />
        protected override IEnumerable<CosmosDbDocument> FindRows(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            if (!_Related.TryGetValue(column, out Dictionary<object, IReadOnlyList<CosmosDbDocument>>? byKey))
            {
                byKey = new Dictionary<object, IReadOnlyList<CosmosDbDocument>>(QueryValueComparer.Ordinal!);
                _Related[column] = byKey;
            }

            if (byKey.TryGetValue(key, out IReadOnlyList<CosmosDbDocument>? cached)) return cached;
            List<CosmosDbDocument> rows = new List<CosmosDbDocument>();
            foreach (CosmosDbDocument candidate in _FindRows(metadata, column, key))
            {
                if (QueryValueComparer.Ordinal.Equals(candidate.Get(column), key)) rows.Add(candidate);
            }

            byKey[key] = rows;
            return rows;
        }

        /// <inheritdoc />
        protected override object? NormalizeValue(ColumnMetadata column, object? value)
        {
            return _Values.ToStored(column, value);
        }
    }
}
