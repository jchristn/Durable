namespace Durable.MongoDb
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.Query;
    using MongoDB.Bson;
    using MongoDB.Bson.IO;

    /// <summary>
    /// The server-side part of a query: the filter is split into AND-ed conjuncts, each translated by
    /// <see cref="MongoDbFilterTranslator"/> when possible. Every pushed conjunct is a necessary condition of the filter, so
    /// evaluating the full filter client-side afterwards never changes results; when every conjunct translated exactly,
    /// <see cref="Exact"/> is true and MongoDB's result is final (paging, counting and aggregates can then run on the
    /// server too). When every key column is compared for equality, an <c>_id</c> equality on the composite key is added so
    /// the lookup uses the <c>_id</c> index.
    /// Thread safety: immutable once built; safe for concurrent use.
    /// </summary>
    internal sealed class MongoDbPushdown
    {
        #region Public-Members

        /// <summary>
        /// Gets the pushed conjuncts (ANDed). Never null; empty when every document is read.
        /// </summary>
        public IReadOnlyList<BsonDocument> Conjuncts => _Conjuncts;

        /// <summary>
        /// Gets whether the conjuncts are exactly the filter.
        /// </summary>
        public bool Exact { get; private set; } = true;

        #endregion

        #region Private-Members

        private static readonly JsonWriterSettings _Json = new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson, Indent = false };
        private readonly List<BsonDocument> _Conjuncts = new List<BsonDocument>();

        #endregion

        #region Constructors-and-Factories

        private MongoDbPushdown()
        {
        }

        /// <summary>
        /// Translates a filter.
        /// </summary>
        /// <param name="source">Source the filter's own columns refer to. Must not be null.</param>
        /// <param name="filter">Filter; null pushes nothing and is exact.</param>
        /// <param name="schema">Schema of the source's entity. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <returns>The translation. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when source, schema or values is null.</exception>
        public static MongoDbPushdown Translate(QuerySource source, QueryNode? filter, MongoDbCollectionSchema schema, MongoDbValueConverter values)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(values);
            MongoDbPushdown pushdown = new MongoDbPushdown();
            if (filter == null) return pushdown;

            MongoDbFilterTranslator translator = new MongoDbFilterTranslator(source, schema, values);
            List<QueryNode> conjuncts = new List<QueryNode>();
            Flatten(filter, conjuncts);
            Dictionary<ColumnMetadata, object> keyEqualities = new Dictionary<ColumnMetadata, object>(ReferenceEqualityComparer.Instance);
            foreach (QueryNode conjunct in conjuncts)
            {
                MongoDbFilterPart? part = translator.Visit(conjunct);
                if (part == null)
                {
                    pushdown.Exact = false;
                    continue;
                }

                if (part.Filter.ElementCount > 0) pushdown._Conjuncts.Add(part.Filter);
                if (!part.Exact) pushdown.Exact = false;
                CollectKeyEquality(translator, conjunct, schema, values, keyEqualities);
            }

            pushdown.AddCompositeKeyPredicate(schema, keyEqualities);
            return pushdown;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds one MongoDB filter ANDing every conjunct.
        /// </summary>
        /// <returns>The filter; an empty document when nothing was pushed. Never null.</returns>
        public BsonDocument Combined()
        {
            if (_Conjuncts.Count == 0) return new BsonDocument();
            if (_Conjuncts.Count == 1) return _Conjuncts[0];
            return new BsonDocument("$and", new BsonArray(_Conjuncts));
        }

        /// <summary>
        /// Returns the conjuncts as relaxed extended JSON, for query plans.
        /// </summary>
        /// <returns>The JSON texts. Never null.</returns>
        public List<string> Describe()
        {
            return _Conjuncts.Select(ToJson).ToList();
        }

        /// <summary>
        /// Renders a BSON document as relaxed extended JSON.
        /// </summary>
        /// <param name="document">Document. Must not be null.</param>
        /// <returns>The JSON text.</returns>
        public static string ToJson(BsonDocument document)
        {
            return document.ToJson(_Json);
        }

        #endregion

        #region Private-Methods

        private static void Flatten(QueryNode node, List<QueryNode> conjuncts)
        {
            if (node is LogicalNode logical && logical.Operator == LogicalOperator.And)
            {
                Flatten(logical.Left, conjuncts);
                Flatten(logical.Right, conjuncts);
                return;
            }

            conjuncts.Add(node);
        }

        private static void CollectKeyEquality(MongoDbFilterTranslator translator, QueryNode conjunct, MongoDbCollectionSchema schema, MongoDbValueConverter values, Dictionary<ColumnMetadata, object> keyEqualities)
        {
            if (schema.SingleKey || conjunct is not ComparisonNode comparison || comparison.Operator != ComparisonOperator.Equal) return;
            if (comparison.StringMode == StringMatchMode.IgnoreCase && (comparison.Left.ClrType == typeof(string) || comparison.Right.ClrType == typeof(string))) return;
            ColumnMetadata? column = translator.OwnColumn(comparison.Left);
            ValueNode? value = comparison.Right as ValueNode;
            if (column == null)
            {
                column = translator.OwnColumn(comparison.Right);
                value = comparison.Left as ValueNode;
            }

            if (column == null || value == null || !column.IsPrimaryKey) return;
            Type stored = schema.StoredType(column);
            if (MongoDbBsonCodec.IsRanged(stored)) return;
            object? parameter = value.Column != null ? values.ToStored(value.Column, value.Value) : value.Value;
            if (parameter == null || parameter.GetType() != stored) return;
            keyEqualities[column] = parameter;
        }

        private void AddCompositeKeyPredicate(MongoDbCollectionSchema schema, Dictionary<ColumnMetadata, object> keyEqualities)
        {
            if (schema.SingleKey || keyEqualities.Count != schema.Metadata.KeyColumns.Count) return;
            object?[] key = new object?[schema.Metadata.KeyColumns.Count];
            for (int i = 0; i < key.Length; i++)
            {
                if (!keyEqualities.TryGetValue(schema.Metadata.KeyColumns[i], out object? part)) return;
                key[i] = part;
            }

            _Conjuncts.Insert(0, new BsonDocument(MongoDbCollectionSchema.IdField, schema.Id(key)));
        }

        #endregion
    }
}
