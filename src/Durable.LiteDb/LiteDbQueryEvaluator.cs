namespace Durable.LiteDb
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;
    using LiteDB;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees client-side against LiteDB documents with the C# semantics of
    /// <see cref="QueryEvaluator{TRow}"/>: everything a <see cref="LiteDbBackend"/> does not push down (residual
    /// predicates, navigations, collection predicates, computed values, ordering, paging and aggregates). Values are
    /// compared in their stored form (enum names, converter provider values, JSON text): fields are decoded with
    /// <see cref="LiteDbBsonCodec"/> and parameter values converted with <see cref="LiteDbValueConverter.ToStored"/>.
    /// Related documents are read from LiteDB by <c>_id</c> or by an indexed field and cached for the evaluator's lifetime.
    /// Thread safety: not thread-safe; create one per operation and use it on the thread that runs the operation.
    /// </summary>
    internal sealed class LiteDbQueryEvaluator : QueryEvaluator<BsonDocument>
    {
        private readonly LiteDatabase _Database;
        private readonly LiteDbValueConverter _Values;
        private readonly Dictionary<ColumnMetadata, Dictionary<object, List<BsonDocument>>> _Related = new Dictionary<ColumnMetadata, Dictionary<object, List<BsonDocument>>>(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Instantiates an evaluator.
        /// </summary>
        /// <param name="database">Database related documents are read from. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public LiteDbQueryEvaluator(LiteDatabase database, LiteDbValueConverter values)
        {
            _Database = database ?? throw new ArgumentNullException(nameof(database));
            _Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        /// <inheritdoc />
        protected override object? GetValue(EntityMetadata metadata, BsonDocument row, ColumnMetadata column)
        {
            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(metadata);
            return LiteDbBsonCodec.Decode(row[schema.Field(column)], schema.StoredType(column));
        }

        /// <inheritdoc />
        protected override IEnumerable<BsonDocument> FindRows(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            if (!_Related.TryGetValue(column, out Dictionary<object, List<BsonDocument>>? byKey))
            {
                byKey = new Dictionary<object, List<BsonDocument>>(QueryValueComparer.Ordinal!);
                _Related[column] = byKey;
            }

            if (byKey.TryGetValue(key, out List<BsonDocument>? cached)) return cached;

            LiteDbCollectionSchema schema = LiteDbCollectionSchema.For(metadata);
            ILiteCollection<BsonDocument> collection = _Database.GetCollection(schema.CollectionName);
            string field = schema.Field(column);
            Type stored = schema.StoredType(column);
            bool indexable = !LiteDbBsonCodec.IsRanged(stored) && stored != typeof(byte[])
                && (key.GetType() == stored || (LiteDbBsonCodec.IsIntegral(key.GetType()) && LiteDbBsonCodec.IsIntegral(stored)));
            IEnumerable<BsonDocument> candidates;
            if (!indexable)
            {
                candidates = collection.FindAll();
            }
            else if (field == LiteDbCollectionSchema.IdField)
            {
                BsonDocument? document = collection.FindById(LiteDbBsonCodec.Encode(key));
                candidates = document == null ? Array.Empty<BsonDocument>() : new[] { document };
            }
            else
            {
                BsonDocument parameters = new BsonDocument { ["k"] = LiteDbBsonCodec.Encode(key) };
                candidates = collection.Find(BsonExpression.Create(LiteDbCollectionSchema.FieldPath(field) + " = @k", parameters));
            }

            List<BsonDocument> rows = new List<BsonDocument>();
            foreach (BsonDocument candidate in candidates)
            {
                if (QueryValueComparer.Ordinal.Equals(LiteDbBsonCodec.Decode(candidate[field], stored), key)) rows.Add(candidate);
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
