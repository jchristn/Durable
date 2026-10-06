namespace Durable.LiteDb
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Durable;
    using Durable.Query;
    using LiteDB;

    /// <summary>
    /// Translates the parts of a <see cref="QueryNode"/> filter whose LiteDB semantics are exactly C#'s into LiteDB
    /// <see cref="BsonExpression"/> predicates. The filter is split into AND-ed conjuncts; each conjunct is translated when
    /// it is a comparison, null check or <c>Contains</c> (IN) between a column of the queried source and a client-side
    /// value (or an AND/OR of such), and left to client-side evaluation otherwise. Every pushed predicate is a necessary
    /// condition of the filter (so evaluating the full filter client-side afterwards never changes results); when every
    /// conjunct translated exactly, <see cref="Exact"/> is true and LiteDB's result is final.
    /// <para>
    /// Exactness rules: parameters are converted to the column's stored form and must have the column's stored type;
    /// comparisons with a value of another type are not pushed. Numbers, bool, Guid, TimeSpan, DateOnly and TimeOnly
    /// compare exactly. DateTime and DateTimeOffset compare by their integral tick key (see <see cref="LiteDbBsonCodec"/>).
    /// Strings and chars compare exactly only when the database collation is ordinal and the comparison is ordinal
    /// (<see cref="StringMatchMode.Database"/> or <see cref="StringMatchMode.Ordinal"/>); with another collation only
    /// equality and IN are pushed, as a superset, and the filter is re-evaluated client-side. Because LiteDB orders null
    /// below every value, <c>&lt;</c> and <c>&lt;=</c> are pushed with an explicit <c>!= null</c> guard. Byte arrays,
    /// case-insensitive matches, string functions, navigations and arithmetic are always evaluated client-side.
    /// </para>
    /// Thread safety: immutable once built; safe for concurrent use.
    /// </summary>
    internal sealed class LiteDbPushdown
    {
        #region Public-Members

        /// <summary>
        /// Gets the pushed predicates (ANDed). Never null.
        /// </summary>
        public IReadOnlyList<string> Predicates => _Predicates;

        /// <summary>
        /// Gets the parameters referenced by the predicates. Never null.
        /// </summary>
        public BsonDocument Parameters { get; } = new BsonDocument();

        /// <summary>
        /// Gets whether the predicates are exactly the filter.
        /// </summary>
        public bool Exact { get; private set; } = true;

        /// <summary>
        /// Gets whether anything was pushed.
        /// </summary>
        public bool Any => _Predicates.Count > 0;

        #endregion

        #region Private-Members

        private readonly List<string> _Predicates = new List<string>();
        private readonly QuerySource _Source;
        private readonly LiteDbCollectionSchema _Schema;
        private readonly LiteDbValueConverter _Values;
        private readonly bool _OrdinalStrings;
        private readonly Dictionary<ColumnMetadata, object> _KeyEqualities = new Dictionary<ColumnMetadata, object>(ReferenceEqualityComparer.Instance);

        #endregion

        #region Constructors-and-Factories

        private LiteDbPushdown(QuerySource source, LiteDbCollectionSchema schema, LiteDbValueConverter values, bool ordinalStrings)
        {
            _Source = source;
            _Schema = schema;
            _Values = values;
            _OrdinalStrings = ordinalStrings;
        }

        /// <summary>
        /// Translates a filter.
        /// </summary>
        /// <param name="source">Source the filter's own columns refer to. Must not be null.</param>
        /// <param name="filter">Filter; null pushes nothing and is exact.</param>
        /// <param name="schema">Schema of the source's entity. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <param name="ordinalStrings">Whether the database collation is ordinal.</param>
        /// <returns>The translation. Never null.</returns>
        public static LiteDbPushdown Translate(QuerySource source, QueryNode? filter, LiteDbCollectionSchema schema, LiteDbValueConverter values, bool ordinalStrings)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(values);
            LiteDbPushdown pushdown = new LiteDbPushdown(source, schema, values, ordinalStrings);
            if (filter == null) return pushdown;

            List<QueryNode> conjuncts = new List<QueryNode>();
            Flatten(filter, conjuncts);
            foreach (QueryNode conjunct in conjuncts)
            {
                string? predicate = pushdown.TranslateNode(conjunct, true, out bool exact);
                if (predicate == null)
                {
                    pushdown.Exact = false;
                    continue;
                }

                pushdown._Predicates.Add(predicate);
                if (!exact) pushdown.Exact = false;
            }

            pushdown.AddCompositeKeyPredicate();
            return pushdown;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds one LiteDB expression ANDing every predicate.
        /// </summary>
        /// <returns>The expression, or null when nothing was pushed.</returns>
        public BsonExpression? Combined()
        {
            if (_Predicates.Count == 0) return null;
            string text = _Predicates.Count == 1 ? _Predicates[0] : "(" + string.Join(") AND (", _Predicates) + ")";
            return BsonExpression.Create(text, Parameters);
        }

        /// <summary>
        /// Returns the parameters as JSON for diagnostics.
        /// </summary>
        /// <returns>The JSON, or null when there are no parameters.</returns>
        public string? ParametersJson()
        {
            return Parameters.Count == 0 ? null : JsonSerializer.Serialize(Parameters);
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

        private string? TranslateNode(QueryNode node, bool topLevel, out bool exact)
        {
            exact = false;
            switch (node)
            {
                case LogicalNode logical:
                    {
                        string? left = TranslateNode(logical.Left, false, out bool leftExact);
                        string? right = TranslateNode(logical.Right, false, out bool rightExact);
                        if (logical.Operator == LogicalOperator.Or)
                        {
                            if (left == null || right == null) return null;
                            exact = leftExact && rightExact;
                            return "(" + left + " OR " + right + ")";
                        }

                        if (left != null && right != null)
                        {
                            exact = leftExact && rightExact;
                            return "(" + left + " AND " + right + ")";
                        }

                        return left ?? right;
                    }
                case ComparisonNode comparison:
                    return TranslateComparison(comparison, topLevel, out exact);
                case NullCheckNode nullCheck:
                    {
                        if (!OwnColumn(nullCheck.Operand, out ColumnMetadata? column)) return null;
                        exact = true;
                        return _Schema.Path(column!) + (nullCheck.IsNull ? " = null" : " != null");
                    }
                case InNode membership:
                    return TranslateIn(membership, out exact);
                default:
                    return null;
            }
        }

        private string? TranslateComparison(ComparisonNode node, bool topLevel, out bool exact)
        {
            exact = false;
            ComparisonOperator op = node.Operator;
            ColumnMetadata? column;
            ValueNode? value;
            if (OwnColumn(node.Left, out column) && node.Right is ValueNode right)
            {
                value = right;
            }
            else if (OwnColumn(node.Right, out column) && node.Left is ValueNode left)
            {
                value = left;
                op = Flip(op);
            }
            else
            {
                return null;
            }

            Type stored = _Schema.StoredType(column!);
            object? parameter = value.Column != null ? _Values.ToStored(value.Column, value.Value) : value.Value;
            if (parameter != null && parameter.GetType() != stored) return null;
            string path = _Schema.Path(column!);

            bool strings = node.Left.ClrType == typeof(string) || node.Right.ClrType == typeof(string);
            bool exactStrings = true;
            if (LiteDbBsonCodec.IsString(stored))
            {
                if (strings && node.StringMode == StringMatchMode.IgnoreCase) return null;
                if (!_OrdinalStrings)
                {
                    if (op != ComparisonOperator.Equal) return null;
                    exactStrings = false;
                }
            }
            else if (!LiteDbBsonCodec.IsPlain(stored) && !LiteDbBsonCodec.IsRanged(stored))
            {
                return null;
            }

            if (parameter == null)
            {
                if (op == ComparisonOperator.Equal)
                {
                    exact = true;
                    return path + " = null";
                }

                if (op == ComparisonOperator.NotEqual)
                {
                    exact = true;
                    return path + " != null";
                }

                return null;
            }

            exact = exactStrings;
            if (topLevel && op == ComparisonOperator.Equal && column!.IsPrimaryKey && exactStrings && !LiteDbBsonCodec.IsRanged(stored)) _KeyEqualities[column] = parameter;

            if (LiteDbBsonCodec.IsRanged(stored))
            {
                decimal key = LiteDbBsonCodec.RangeKey(parameter);
                string low = AddParameter(new BsonValue(key));
                string high = AddParameter(new BsonValue(key + 1m));
                switch (op)
                {
                    case ComparisonOperator.Equal: return "(" + path + " >= " + low + " AND " + path + " < " + high + ")";
                    case ComparisonOperator.NotEqual: return "(" + path + " = null OR " + path + " < " + low + " OR " + path + " >= " + high + ")";
                    case ComparisonOperator.GreaterThan: return path + " >= " + high;
                    case ComparisonOperator.GreaterThanOrEqual: return path + " >= " + low;
                    case ComparisonOperator.LessThan: return "(" + path + " != null AND " + path + " < " + low + ")";
                    default: return "(" + path + " != null AND " + path + " < " + high + ")";
                }
            }

            string name = AddParameter(LiteDbBsonCodec.Encode(parameter));
            switch (op)
            {
                case ComparisonOperator.Equal: return path + " = " + name;
                case ComparisonOperator.NotEqual: return path + " != " + name;
                case ComparisonOperator.GreaterThan: return path + " > " + name;
                case ComparisonOperator.GreaterThanOrEqual: return path + " >= " + name;
                case ComparisonOperator.LessThan: return "(" + path + " != null AND " + path + " < " + name + ")";
                default: return "(" + path + " != null AND " + path + " <= " + name + ")";
            }
        }

        private string? TranslateIn(InNode node, out bool exact)
        {
            exact = false;
            if (node.Negated || !OwnColumn(node.Item, out ColumnMetadata? column)) return null;
            Type stored = _Schema.StoredType(column!);
            bool exactStrings = true;
            if (LiteDbBsonCodec.IsString(stored))
            {
                if (node.Item.ClrType == typeof(string) && node.StringMode == StringMatchMode.IgnoreCase) return null;
                exactStrings = _OrdinalStrings;
            }
            else if (!LiteDbBsonCodec.IsPlain(stored))
            {
                return null;
            }

            BsonArray array = new BsonArray();
            foreach (object? raw in node.Values)
            {
                object? value = _Values.ToStored(column!, raw);
                if (value != null && value.GetType() != stored) return null;
                array.Add(LiteDbBsonCodec.Encode(value));
            }

            exact = exactStrings;
            return _Schema.Path(column!) + " IN " + AddParameter(array);
        }

        private void AddCompositeKeyPredicate()
        {
            if (_Schema.SingleKey || _KeyEqualities.Count != _Schema.Metadata.KeyColumns.Count) return;
            object?[] key = new object?[_Schema.Metadata.KeyColumns.Count];
            for (int i = 0; i < key.Length; i++)
            {
                if (!_KeyEqualities.TryGetValue(_Schema.Metadata.KeyColumns[i], out object? part)) return;
                key[i] = part;
            }

            _Predicates.Insert(0, "$._id = " + AddParameter(_Schema.Id(key)));
        }

        private bool OwnColumn(QueryNode node, out ColumnMetadata? column)
        {
            if (node is ColumnNode columnNode && ReferenceEquals(columnNode.Source, _Source))
            {
                column = columnNode.Column;
                return true;
            }

            column = null;
            return false;
        }

        private string AddParameter(BsonValue value)
        {
            string name = "p" + Parameters.Count.ToString(CultureInfo.InvariantCulture);
            Parameters[name] = value;
            return "@" + name;
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
