namespace Durable.MongoDb
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;
    using MongoDB.Bson;
    using MongoDB.Driver;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees client-side against MongoDB documents with the C# semantics of
    /// <see cref="QueryEvaluator{TRow}"/>: everything a <see cref="MongoDbBackend"/> does not push down (residual
    /// predicates, navigations, collection predicates, computed values, and ordering, paging or aggregates over them).
    /// Values are compared in their stored form (enum names, converter provider values, JSON text): fields are decoded with
    /// <see cref="MongoDbBsonCodec"/> and parameter values converted with <see cref="MongoDbValueConverter.ToStored"/>.
    /// Related documents are read from MongoDB (inside the operation's session, when there is one) by <c>_id</c> or by
    /// field and cached for the evaluator's lifetime.
    /// Thread safety: not thread-safe; create one per operation.
    /// </summary>
    internal sealed class MongoDbQueryEvaluator : QueryEvaluator<BsonDocument>
    {
        private readonly IMongoDatabase _Database;
        private readonly IClientSessionHandle? _Session;
        private readonly MongoDbValueConverter _Values;
        private readonly Dictionary<ColumnMetadata, Dictionary<object, List<BsonDocument>>> _Related = new Dictionary<ColumnMetadata, Dictionary<object, List<BsonDocument>>>(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Instantiates an evaluator.
        /// </summary>
        /// <param name="database">Database related documents are read from. Must not be null.</param>
        /// <param name="session">Session of the operation's transaction; null for none.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when database or values is null.</exception>
        public MongoDbQueryEvaluator(IMongoDatabase database, IClientSessionHandle? session, MongoDbValueConverter values)
        {
            _Database = database ?? throw new ArgumentNullException(nameof(database));
            _Session = session;
            _Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        /// <inheritdoc />
        protected override object? GetValue(EntityMetadata metadata, BsonDocument row, ColumnMetadata column)
        {
            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(metadata);
            return MongoDbBsonCodec.Decode(row.GetValue(schema.Field(column), BsonNull.Value), schema.StoredType(column));
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

            MongoDbCollectionSchema schema = MongoDbCollectionSchema.For(metadata);
            IMongoCollection<BsonDocument> collection = _Database.GetCollection<BsonDocument>(schema.CollectionName);
            string field = schema.Field(column);
            Type stored = schema.StoredType(column);
            BsonDocument filter;
            if (MongoDbBsonCodec.IsRanged(stored) && (key is DateTime || key is DateTimeOffset))
            {
                decimal rangeKey = MongoDbBsonCodec.RangeKey(key);
                filter = new BsonDocument(field, new BsonDocument { { "$gte", MongoDbBsonCodec.EncodeDecimal(rangeKey) }, { "$lt", MongoDbBsonCodec.EncodeDecimal(rangeKey + 1m) } });
            }
            else if (key.GetType() == stored || (IsIntegral(key.GetType()) && IsIntegral(stored)))
            {
                filter = new BsonDocument(field, new BsonDocument("$eq", MongoDbBsonCodec.Encode(key)));
            }
            else
            {
                filter = new BsonDocument();
            }

            FindOptions options = new FindOptions { Collation = Collation.Simple };
            List<BsonDocument> candidates = _Session != null
                ? collection.Find(_Session, filter, options).ToList()
                : collection.Find(filter, options).ToList();

            List<BsonDocument> rows = new List<BsonDocument>();
            foreach (BsonDocument candidate in candidates)
            {
                if (QueryValueComparer.Ordinal.Equals(MongoDbBsonCodec.Decode(candidate.GetValue(field, BsonNull.Value), stored), key)) rows.Add(candidate);
            }

            byKey[key] = rows;
            return rows;
        }

        /// <inheritdoc />
        protected override object? NormalizeValue(ColumnMetadata column, object? value)
        {
            return _Values.ToStored(column, value);
        }

        private static bool IsIntegral(Type type)
        {
            return type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)
                || type == typeof(sbyte) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong);
        }
    }
}
