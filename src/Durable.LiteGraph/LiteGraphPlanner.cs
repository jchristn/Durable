namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;
    using ExpressionTree;

    /// <summary>
    /// Decides what part of a filter is pushed down to LiteGraph. Only conditions whose LiteGraph evaluation is exactly
    /// implied by the C# semantics are pushed, and only from the top-level conjunction (so a pushed condition is necessary
    /// for a row to match): equality on every primary key column (or a key <c>Contains</c> list for single-column keys)
    /// becomes a lookup by deterministic node GUIDs; with data push-down enabled, ordinal string equality and string
    /// <c>Contains</c> lists on root columns become ExpressionTree <c>Equals</c>/<c>In</c> data filters, and equality with
    /// a non-negative integer becomes an <c>Equals</c> filter (integer lists become an <c>Or</c> of equalities). Strings
    /// with control or surrogate characters, case-insensitive comparisons, negative numbers (whose culture-dependent
    /// formatting LiteGraph would inline) and everything else stay client-side. The complete filter is always evaluated
    /// client-side afterwards, so push-down only narrows the candidate nodes.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class LiteGraphPlanner
    {
        #region Private-Members

        private readonly LiteGraphValueConverter _Values;
        private readonly Guid _GraphGuid;
        private readonly bool _PushDownData;
        private readonly int _MaxIntegerList = 64;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a planner.
        /// </summary>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <param name="graphGuid">Graph GUID (for node GUIDs).</param>
        /// <param name="pushDownData">Whether data filters are pushed down.</param>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public LiteGraphPlanner(LiteGraphValueConverter values, Guid graphGuid, bool pushDownData)
        {
            _Values = values ?? throw new ArgumentNullException(nameof(values));
            _GraphGuid = graphGuid;
            _PushDownData = pushDownData;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Plans the read of a filter's candidate rows.
        /// </summary>
        /// <param name="source">Root source of the filter. Must not be null.</param>
        /// <param name="filter">Filter; null selects every row.</param>
        /// <returns>The request. Never null.</returns>
        public LiteGraphReadRequest Plan(QuerySource source, QueryNode? filter)
        {
            ArgumentNullException.ThrowIfNull(source);
            EntityMetadata metadata = source.Metadata;
            List<QueryNode> conjuncts = new List<QueryNode>();
            Flatten(filter, conjuncts);

            List<Guid>? guids = KeyLookup(source, conjuncts);
            if (guids != null) return new LiteGraphReadRequest(metadata, guids, null);

            Expr? expression = null;
            if (_PushDownData)
            {
                foreach (QueryNode conjunct in conjuncts)
                {
                    Expr? part = DataFilter(source, conjunct);
                    if (part == null) continue;
                    expression = expression == null ? part : new Expr(expression, OperatorEnum.And, part);
                }
            }

            return new LiteGraphReadRequest(metadata, null, expression);
        }

        /// <summary>
        /// Returns the data filter selecting rows whose column equals a stored value, when it can be pushed down exactly.
        /// </summary>
        /// <param name="column">Root column. Must not be null.</param>
        /// <param name="stored">Stored value. Must not be null.</param>
        /// <returns>The filter, or null when it cannot be pushed down.</returns>
        public Expr? Equality(ColumnMetadata column, object stored)
        {
            if (!_PushDownData || !SafeName(column.Name)) return null;
            if (stored is string text) return SafeString(text) ? new Expr(column.Name, OperatorEnum.Equals, text) : null;
            long? number = NonNegativeInteger(stored);
            if (number.HasValue && IsInteger(LiteGraphValueConverter.StoredType(column))) return new Expr(column.Name, OperatorEnum.Equals, number.Value);
            return null;
        }

        #endregion

        #region Private-Methods

        private List<Guid>? KeyLookup(QuerySource source, List<QueryNode> conjuncts)
        {
            EntityMetadata metadata = source.Metadata;
            if (metadata.KeyColumns.Count == 0) return null;

            object?[] key = new object?[metadata.KeyColumns.Count];
            foreach (QueryNode conjunct in conjuncts)
            {
                if (conjunct is not ComparisonNode comparison || comparison.Operator != ComparisonOperator.Equal) continue;
                if (!ColumnAndValue(source, comparison, out ColumnNode? column, out ValueNode? value)) continue;
                if (!column!.Column.IsPrimaryKey || column.Column.KeyOrdinal < 0) continue;
                object? stored = Stored(column.Column, value!.Value);
                if (stored != null) key[column.Column.KeyOrdinal] = stored;
            }

            bool complete = true;
            foreach (object? part in key) complete &= part != null;
            if (complete) return new List<Guid> { LiteGraphIdentity.Node(_GraphGuid, metadata.TableName, key) };

            if (metadata.KeyColumns.Count != 1) return null;
            ColumnMetadata keyColumn = metadata.KeyColumns[0];
            foreach (QueryNode conjunct in conjuncts)
            {
                if (conjunct is not InNode membership || membership.Negated) continue;
                if (membership.Item is not ColumnNode item || !ReferenceEquals(item.Source, source) || !ReferenceEquals(item.Column, keyColumn)) continue;

                List<Guid> guids = new List<Guid>();
                HashSet<Guid> seen = new HashSet<Guid>();
                bool convertible = true;
                foreach (object? raw in membership.Values)
                {
                    if (raw == null) continue;
                    object? stored = Stored(keyColumn, raw);
                    if (stored == null)
                    {
                        convertible = false;
                        break;
                    }

                    Guid guid = LiteGraphIdentity.Node(_GraphGuid, metadata.TableName, new[] { stored });
                    if (seen.Add(guid)) guids.Add(guid);
                }

                if (convertible) return guids;
            }

            return null;
        }

        private Expr? DataFilter(QuerySource source, QueryNode conjunct)
        {
            if (conjunct is ComparisonNode comparison && comparison.Operator == ComparisonOperator.Equal)
            {
                if (!ColumnAndValue(source, comparison, out ColumnNode? column, out ValueNode? value)) return null;
                bool stringComparison = comparison.Left.ClrType == typeof(string) || comparison.Right.ClrType == typeof(string);
                if (stringComparison && comparison.StringMode == StringMatchMode.IgnoreCase) return null;
                object? stored = Stored(column!.Column, value!.Value);
                return stored == null ? null : Equality(column.Column, stored);
            }

            if (conjunct is InNode membership && !membership.Negated)
            {
                if (membership.Item is not ColumnNode item || !ReferenceEquals(item.Source, source)) return null;
                ColumnMetadata column = item.Column;
                if (!SafeName(column.Name) || membership.Values.Count == 0) return null;
                if (membership.Item.ClrType == typeof(string) && membership.StringMode == StringMatchMode.IgnoreCase) return null;

                List<object> strings = new List<object>();
                List<long> numbers = new List<long>();
                foreach (object? raw in membership.Values)
                {
                    if (raw == null) return null;
                    object? stored = Stored(column, raw);
                    if (stored is string text && SafeString(text))
                    {
                        strings.Add(text);
                        continue;
                    }

                    long? number = stored == null ? null : NonNegativeInteger(stored);
                    if (!number.HasValue) return null;
                    numbers.Add(number.Value);
                }

                if (strings.Count > 0 && numbers.Count == 0) return new Expr(column.Name, OperatorEnum.In, strings);
                if (numbers.Count > 0 && strings.Count == 0 && numbers.Count <= _MaxIntegerList && IsInteger(LiteGraphValueConverter.StoredType(column)))
                {
                    Expr? any = null;
                    foreach (long number in numbers)
                    {
                        Expr equal = new Expr(column.Name, OperatorEnum.Equals, number);
                        any = any == null ? equal : new Expr(any, OperatorEnum.Or, equal);
                    }

                    return any;
                }
            }

            return null;
        }

        private object? Stored(ColumnMetadata column, object? value)
        {
            if (value == null) return null;
            try
            {
                object? stored = _Values.ToStored(column, value);
                if (stored == null || stored.GetType() != LiteGraphValueConverter.StoredType(column)) return null;
                return stored;
            }
            catch (Exception e) when (e is ArgumentException || e is FormatException || e is InvalidCastException || e is OverflowException || e is NotSupportedException || e is System.Text.Json.JsonException)
            {
                return null;
            }
        }

        private static bool ColumnAndValue(QuerySource source, ComparisonNode comparison, out ColumnNode? column, out ValueNode? value)
        {
            column = comparison.Left as ColumnNode ?? comparison.Right as ColumnNode;
            value = comparison.Right as ValueNode ?? comparison.Left as ValueNode;
            if (column == null || value == null) return false;
            if (!ReferenceEquals(column.Source, source)) return false;
            return value.Column != null && ReferenceEquals(value.Column, column.Column) && value.Value != null;
        }

        private static void Flatten(QueryNode? node, List<QueryNode> into)
        {
            if (node == null) return;
            if (node is LogicalNode logical && logical.Operator == LogicalOperator.And)
            {
                Flatten(logical.Left, into);
                Flatten(logical.Right, into);
                return;
            }

            into.Add(node);
        }

        private static long? NonNegativeInteger(object value)
        {
            switch (value)
            {
                case byte n: return n;
                case sbyte n: return n >= 0 ? n : null;
                case short n: return n >= 0 ? n : null;
                case ushort n: return n;
                case int n: return n >= 0 ? n : null;
                case uint n: return n;
                case long n: return n >= 0 ? n : null;
                default: return null;
            }
        }

        private static bool IsInteger(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long);
        }

        private static bool SafeName(string name)
        {
            if (string.IsNullOrEmpty(name)) return false;
            for (int i = 0; i < name.Length; i++)
            {
                char c = name[i];
                bool letter = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
                bool digit = c >= '0' && c <= '9';
                if (!letter && !(digit && i > 0)) return false;
            }

            return true;
        }

        private static bool SafeString(string text)
        {
            foreach (char c in text)
            {
                if (c < ' ' || char.IsSurrogate(c)) return false;
            }

            return true;
        }

        #endregion
    }
}
