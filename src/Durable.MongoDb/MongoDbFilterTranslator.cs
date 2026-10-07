namespace Durable.MongoDb
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;
    using MongoDB.Bson;

    /// <summary>
    /// Translates <see cref="QueryNode"/> predicates into MongoDB query filters with C# semantics. A node translates when
    /// it is a comparison, null check, <c>Contains</c> (IN), string match, boolean column or constant over columns of the
    /// queried source and client-side values (or an AND/OR/NOT of such); everything else (functions, arithmetic,
    /// navigations, column-to-column comparisons) returns null and is left to client-side evaluation.
    /// <para>
    /// Exactness rules (a translation is exact when MongoDB matches exactly the documents the predicate is true for):
    /// values are converted to the column's stored form and must have the column's stored type. Numbers, bool, Guid,
    /// TimeSpan, DateOnly and TimeOnly compare exactly (MongoDB excludes null from range comparisons and matches null in
    /// <c>$ne</c> and <c>$nin</c>, as C# does). DateTime and DateTimeOffset compare by their integral tick key (see
    /// <see cref="MongoDbBsonCodec"/>). Byte arrays support equality only. Strings use no collation: ordinal (and
    /// <see cref="StringMatchMode.Database"/>) equality, IN and matches are exact; ordinal range comparisons are exact when
    /// the value has no character at or above U+D800 (see <see cref="MongoDbRegex.IsRangeSafe"/>); case-insensitive
    /// equality, IN and matches use exact character-class regexes (see <see cref="MongoDbRegex"/>); case-insensitive range
    /// comparisons are not translated. NOT is translated only around an exact translation; OR only when both sides
    /// translate; an AND with one untranslatable side keeps the other side as an inexact (necessary) condition.
    /// </para>
    /// Thread safety: not thread-safe; create one per translation.
    /// </summary>
    internal sealed class MongoDbFilterTranslator : QueryNodeVisitor<MongoDbFilterPart?>
    {
        #region Private-Members

        private static readonly BsonDocument _MatchNothing = new BsonDocument(MongoDbCollectionSchema.IdField, new BsonDocument("$in", new BsonArray()));
        private readonly QuerySource _Source;
        private readonly MongoDbCollectionSchema _Schema;
        private readonly MongoDbValueConverter _Values;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a translator.
        /// </summary>
        /// <param name="source">Source whose columns are translated. Must not be null.</param>
        /// <param name="schema">Schema of the source's entity. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public MongoDbFilterTranslator(QuerySource source, MongoDbCollectionSchema schema, MongoDbValueConverter values)
        {
            _Source = source ?? throw new ArgumentNullException(nameof(source));
            _Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Gets a filter that matches no document.
        /// </summary>
        /// <returns>A new filter. Never null.</returns>
        public static BsonDocument MatchNothing()
        {
            return (BsonDocument)_MatchNothing.DeepClone();
        }

        /// <summary>
        /// Returns whether a node is a column of the translated source, and the column.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The column, or null.</returns>
        public ColumnMetadata? OwnColumn(QueryNode node)
        {
            return node is ColumnNode column && ReferenceEquals(column.Source, _Source) ? column.Column : null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitColumn(ColumnNode node)
        {
            ColumnMetadata? column = OwnColumn(node);
            if (column == null || _Schema.StoredType(column) != typeof(bool)) return null;
            return Exact(new BsonDocument(_Schema.Field(column), BsonBoolean.True));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitValue(ValueNode node)
        {
            if (node.Value is bool flag) return Exact(flag ? new BsonDocument() : MatchNothing());
            if (node.Value == null && (node.ClrType == typeof(bool) || node.ClrType == typeof(bool?))) return Exact(MatchNothing());
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitLogical(LogicalNode node)
        {
            MongoDbFilterPart? left = Visit(node.Left);
            MongoDbFilterPart? right = Visit(node.Right);
            if (node.Operator == LogicalOperator.Or)
            {
                if (left == null || right == null) return null;
                return new MongoDbFilterPart(new BsonDocument("$or", new BsonArray { left.Filter, right.Filter }), left.Exact && right.Exact);
            }

            if (left != null && right != null)
                return new MongoDbFilterPart(new BsonDocument("$and", new BsonArray { left.Filter, right.Filter }), left.Exact && right.Exact);
            MongoDbFilterPart? either = left ?? right;
            return either == null ? null : new MongoDbFilterPart(either.Filter, false);
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitNot(NotNode node)
        {
            MongoDbFilterPart? inner = Visit(node.Operand);
            if (inner == null || !inner.Exact) return null;
            return Exact(new BsonDocument("$nor", new BsonArray { inner.Filter }));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitNullCheck(NullCheckNode node)
        {
            ColumnMetadata? column = OwnColumn(node.Operand);
            if (column == null) return null;
            string field = _Schema.Field(column);
            return Exact(node.IsNull ? new BsonDocument(field, BsonNull.Value) : new BsonDocument(field, new BsonDocument("$ne", BsonNull.Value)));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitComparison(ComparisonNode node)
        {
            ComparisonOperator op = node.Operator;
            ColumnMetadata? column;
            ValueNode? value;
            if ((column = OwnColumn(node.Left)) != null && node.Right is ValueNode right)
            {
                value = right;
            }
            else if ((column = OwnColumn(node.Right)) != null && node.Left is ValueNode left)
            {
                value = left;
                op = Flip(op);
            }
            else
            {
                return null;
            }

            Type stored = _Schema.StoredType(column);
            object? parameter = value.Column != null ? _Values.ToStored(value.Column, value.Value) : value.Value;
            if (parameter != null && parameter.GetType() != stored) return null;
            string field = _Schema.Field(column);
            bool ignoreCase = (node.Left.ClrType == typeof(string) || node.Right.ClrType == typeof(string)) && node.StringMode == StringMatchMode.IgnoreCase;

            if (parameter == null)
            {
                if (op == ComparisonOperator.Equal) return Exact(new BsonDocument(field, BsonNull.Value));
                if (op == ComparisonOperator.NotEqual) return Exact(new BsonDocument(field, new BsonDocument("$ne", BsonNull.Value)));
                return Exact(MatchNothing());
            }

            if (MongoDbBsonCodec.IsString(stored))
            {
                string text = parameter is char c ? c.ToString() : (string)parameter;
                if (ignoreCase && stored == typeof(string))
                {
                    if (op != ComparisonOperator.Equal && op != ComparisonOperator.NotEqual) return null;
                    BsonRegularExpression? regex = MongoDbRegex.Build(text, null, true);
                    if (regex == null) return null;
                    return Exact(op == ComparisonOperator.Equal
                        ? new BsonDocument(field, regex)
                        : new BsonDocument(field, new BsonDocument("$not", regex)));
                }

                if (op != ComparisonOperator.Equal && op != ComparisonOperator.NotEqual && !MongoDbRegex.IsRangeSafe(text)) return null;
                return Exact(Compare(field, op, new BsonString(text)));
            }

            if (MongoDbBsonCodec.IsRanged(stored))
            {
                decimal key = MongoDbBsonCodec.RangeKey(parameter);
                BsonValue low = MongoDbBsonCodec.EncodeDecimal(key);
                BsonValue high = MongoDbBsonCodec.EncodeDecimal(key + 1m);
                switch (op)
                {
                    case ComparisonOperator.Equal: return Exact(new BsonDocument(field, new BsonDocument { { "$gte", low }, { "$lt", high } }));
                    case ComparisonOperator.NotEqual: return Exact(new BsonDocument(field, new BsonDocument("$not", new BsonDocument { { "$gte", low }, { "$lt", high } })));
                    case ComparisonOperator.GreaterThan: return Exact(new BsonDocument(field, new BsonDocument("$gte", high)));
                    case ComparisonOperator.GreaterThanOrEqual: return Exact(new BsonDocument(field, new BsonDocument("$gte", low)));
                    case ComparisonOperator.LessThan: return Exact(new BsonDocument(field, new BsonDocument("$lt", low)));
                    default: return Exact(new BsonDocument(field, new BsonDocument("$lt", high)));
                }
            }

            if (stored == typeof(byte[]))
            {
                if (op != ComparisonOperator.Equal && op != ComparisonOperator.NotEqual) return null;
                return Exact(Compare(field, op, MongoDbBsonCodec.Encode(parameter)));
            }

            if (!MongoDbBsonCodec.IsPlain(stored)) return null;
            return Exact(Compare(field, op, MongoDbBsonCodec.Encode(parameter)));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitIn(InNode node)
        {
            ColumnMetadata? column = OwnColumn(node.Item);
            if (column == null) return null;
            Type stored = _Schema.StoredType(column);
            bool ignoreCase = node.Item.ClrType == typeof(string) && node.StringMode == StringMatchMode.IgnoreCase && stored == typeof(string);
            if (!MongoDbBsonCodec.IsString(stored) && !MongoDbBsonCodec.IsPlain(stored) && stored != typeof(byte[]) && !MongoDbBsonCodec.IsRanged(stored)) return null;

            BsonArray array = new BsonArray();
            List<BsonValue> ranges = new List<BsonValue>();
            foreach (object? raw in node.Values)
            {
                object? value = _Values.ToStored(column, raw);
                if (value != null && value.GetType() != stored) return null;
                if (value == null)
                {
                    array.Add(BsonNull.Value);
                }
                else if (ignoreCase)
                {
                    BsonRegularExpression? regex = MongoDbRegex.Build((string)value, null, true);
                    if (regex == null) return null;
                    array.Add(regex);
                }
                else if (MongoDbBsonCodec.IsRanged(stored))
                {
                    decimal key = MongoDbBsonCodec.RangeKey(value);
                    ranges.Add(new BsonDocument { { "$gte", MongoDbBsonCodec.EncodeDecimal(key) }, { "$lt", MongoDbBsonCodec.EncodeDecimal(key + 1m) } });
                }
                else
                {
                    array.Add(MongoDbBsonCodec.Encode(value));
                }
            }

            string field = _Schema.Field(column);
            if (ranges.Count > 0)
            {
                BsonArray alternatives = new BsonArray();
                if (array.Count > 0) alternatives.Add(new BsonDocument(field, new BsonDocument("$in", array)));
                foreach (BsonValue range in ranges) alternatives.Add(new BsonDocument(field, range));
                BsonDocument any = alternatives.Count == 1 ? alternatives[0].AsBsonDocument : new BsonDocument("$or", alternatives);
                return Exact(node.Negated ? new BsonDocument("$nor", new BsonArray { any }) : any);
            }

            return Exact(new BsonDocument(field, new BsonDocument(node.Negated ? "$nin" : "$in", array)));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitStringMatch(StringMatchNode node)
        {
            ColumnMetadata? column = OwnColumn(node.Target);
            if (column == null || _Schema.StoredType(column) != typeof(string) || node.Pattern is not ValueNode pattern) return null;
            object? raw = pattern.Value;
            if (raw == null) return Exact(MatchNothing());
            if (raw is not string text) return null;
            BsonRegularExpression? regex = MongoDbRegex.Build(text, node.Kind, node.Mode == StringMatchMode.IgnoreCase);
            return regex == null ? null : Exact(new BsonDocument(_Schema.Field(column), regex));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitStringTest(StringTestNode node)
        {
            ColumnMetadata? column = OwnColumn(node.Operand);
            if (column == null || node.Kind != StringTestKind.IsNullOrEmpty || _Schema.StoredType(column) != typeof(string)) return null;
            string field = _Schema.Field(column);
            return Exact(new BsonDocument(field, new BsonDocument("$in", new BsonArray { BsonNull.Value, new BsonString(string.Empty) })));
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitArithmetic(ArithmeticNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitNegate(NegateNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitConcat(ConcatNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitCoalesce(CoalesceNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitConditional(ConditionalNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitFunction(FunctionNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitNavigationMember(NavigationMemberNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitCollection(CollectionNode node)
        {
            return null;
        }

        /// <inheritdoc />
        public override MongoDbFilterPart? VisitAggregate(AggregateNode node)
        {
            return null;
        }

        #endregion

        #region Private-Methods

        private static MongoDbFilterPart Exact(BsonDocument filter)
        {
            return new MongoDbFilterPart(filter, true);
        }

        private static BsonDocument Compare(string field, ComparisonOperator op, BsonValue value)
        {
            switch (op)
            {
                case ComparisonOperator.Equal: return new BsonDocument(field, new BsonDocument("$eq", value));
                case ComparisonOperator.NotEqual: return new BsonDocument(field, new BsonDocument("$ne", value));
                case ComparisonOperator.GreaterThan: return new BsonDocument(field, new BsonDocument("$gt", value));
                case ComparisonOperator.GreaterThanOrEqual: return new BsonDocument(field, new BsonDocument("$gte", value));
                case ComparisonOperator.LessThan: return new BsonDocument(field, new BsonDocument("$lt", value));
                default: return new BsonDocument(field, new BsonDocument("$lte", value));
            }
        }

        private static ComparisonOperator Flip(ComparisonOperator op)
        {
            switch (op)
            {
                case ComparisonOperator.LessThan: return ComparisonOperator.GreaterThan;
                case ComparisonOperator.LessThanOrEqual: return ComparisonOperator.GreaterThanOrEqual;
                case ComparisonOperator.GreaterThan: return ComparisonOperator.LessThan;
                case ComparisonOperator.GreaterThanOrEqual: return ComparisonOperator.LessThanOrEqual;
                default: return op;
            }
        }

        #endregion
    }
}
