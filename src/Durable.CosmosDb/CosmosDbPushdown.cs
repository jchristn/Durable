namespace Durable.CosmosDb
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;
    using System.Text.Json;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Translates the parts of a <see cref="QueryNode"/> filter whose Cosmos DB semantics match C# into a parameterized
    /// Cosmos DB SQL condition. The filter is split into AND-ed conjuncts; each conjunct is translated when it is built from
    /// comparisons, null checks, <c>Contains</c> (IN) lists, string matches (<c>StartsWith</c>, <c>EndsWith</c>,
    /// <c>Contains</c>) and <c>IsNullOrEmpty</c> on a column of the queried source against client-side values, combined with
    /// AND, OR and NOT; anything else (functions, arithmetic, navigations, columns compared with columns) is left to
    /// client-side evaluation. Every translated condition is total (true or false, never undefined, guarded by
    /// <c>IS_NUMBER</c> / <c>IS_STRING</c> / <c>IS_BOOL</c>, with C# null semantics through <c>IS_DEFINED</c> /
    /// <c>IS_NULL</c>) and a necessary condition of the filter, so evaluating the full filter client-side afterwards never
    /// changes results; when every conjunct translated exactly, <see cref="Exact"/> is true and Cosmos DB's result is final.
    /// <para>
    /// Exactness rules: parameters are converted to the column's stored form and must have its stored type (integers may
    /// mix widths). Integers, booleans, Guids, dates, DateTimeOffset instants, floats and doubles compare exactly (NaN and
    /// infinities, stored as strings, are included explicitly where C# orders them). Strings compare exactly for equality,
    /// string matches and IN; ordering comparisons are exact when the parameter has no character at or above U+D800 (where
    /// Cosmos DB's code point order differs from UTF-16 ordinal order). DateTime compares by its fixed-width wall-clock prefix
    /// (ticks, ignoring the kind, like C#). Long values beyond plus or minus 2^53 and decimals that a double cannot round-trip
    /// may be compared as doubles inside Cosmos DB: comparisons with such a parameter are widened (non-strict) and re-checked
    /// client-side; comparisons of a decimal column with an exact parameter are exact unless the column holds such decimals,
    /// which <see cref="UncertainDecimals"/> lets the backend probe for (translate again with
    /// <c>conservativeDecimals</c> when it does). Case-insensitive matches are pushed with Cosmos DB's case-insensitive
    /// functions only for ASCII patterns without i, I, s or S (whose case folding differs from
    /// <see cref="StringComparison.OrdinalIgnoreCase"/>) and are always re-checked client-side. Byte arrays support
    /// equality only. Columns with a value converter compare their provider values.
    /// </para>
    /// Thread safety: not thread-safe while translating; immutable afterwards.
    /// </summary>
    internal sealed class CosmosDbPushdown : QueryNodeVisitor<CosmosDbSqlFragment?>
    {
        #region Public-Members

        /// <summary>
        /// Gets the pushed conditions (ANDed). Never null.
        /// </summary>
        public IReadOnlyList<string> Predicates => _Predicates;

        /// <summary>
        /// Gets the parameters referenced by the conditions, in order. Never null.
        /// </summary>
        public IReadOnlyList<KeyValuePair<string, object>> Parameters => _Parameters;

        /// <summary>
        /// Gets whether the conditions are exactly the filter.
        /// </summary>
        public bool Exact { get; private set; } = true;

        /// <summary>
        /// Gets the decimal columns whose comparisons were translated exactly on the assumption that the column holds no
        /// decimal a double cannot round-trip. Never null.
        /// </summary>
        public IReadOnlyCollection<ColumnMetadata> UncertainDecimals => _UncertainDecimals;

        /// <summary>
        /// Gets the stored values of the top-level equality conditions on columns of the queried source (exact kinds only),
        /// used for point reads and single-partition queries. Never null.
        /// </summary>
        public IReadOnlyDictionary<ColumnMetadata, object> Equalities => _Equalities;

        #endregion

        #region Private-Members

        private readonly List<string> _Predicates = new List<string>();
        private readonly List<KeyValuePair<string, object>> _Parameters = new List<KeyValuePair<string, object>>();
        private readonly HashSet<ColumnMetadata> _UncertainDecimals = new HashSet<ColumnMetadata>(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<ColumnMetadata, object> _Equalities = new Dictionary<ColumnMetadata, object>(ReferenceEqualityComparer.Instance);
        private readonly QuerySource _Source;
        private readonly CosmosDbContainerSchema _Schema;
        private readonly CosmosDbValueConverter _Values;
        private readonly bool _ConservativeDecimals;
        private bool _TopLevel;

        #endregion

        #region Constructors-and-Factories

        private CosmosDbPushdown(QuerySource source, CosmosDbContainerSchema schema, CosmosDbValueConverter values, bool conservativeDecimals)
        {
            _Source = source;
            _Schema = schema;
            _Values = values;
            _ConservativeDecimals = conservativeDecimals;
        }

        /// <summary>
        /// Translates a filter.
        /// </summary>
        /// <param name="source">Source the filter's own columns refer to. Must not be null.</param>
        /// <param name="filter">Filter; null pushes nothing and is exact.</param>
        /// <param name="schema">Schema of the source's entity. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <param name="conservativeDecimals">Whether decimal columns may hold values a double cannot round-trip (widen every decimal comparison).</param>
        /// <returns>The translation. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when source, schema or values is null.</exception>
        public static CosmosDbPushdown Translate(QuerySource source, QueryNode? filter, CosmosDbContainerSchema schema, CosmosDbValueConverter values, bool conservativeDecimals)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(values);
            CosmosDbPushdown pushdown = new CosmosDbPushdown(source, schema, values, conservativeDecimals);
            if (filter == null) return pushdown;

            List<QueryNode> conjuncts = new List<QueryNode>();
            Flatten(filter, conjuncts);
            foreach (QueryNode conjunct in conjuncts)
            {
                pushdown._TopLevel = true;
                CosmosDbSqlFragment? fragment = pushdown.Visit(conjunct);
                if (fragment == null)
                {
                    pushdown.Exact = false;
                    continue;
                }

                if (fragment.Text != "true") pushdown._Predicates.Add(fragment.Text);
                if (!fragment.Exact) pushdown.Exact = false;
            }

            return pushdown;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the conditions ANDed into one WHERE clause body, or null when nothing was pushed.
        /// </summary>
        /// <returns>The condition, or null.</returns>
        public string? Where()
        {
            if (_Predicates.Count == 0) return null;
            return _Predicates.Count == 1 ? _Predicates[0] : string.Join(" AND ", _Predicates);
        }

        /// <summary>
        /// Adds a parameter and returns its name.
        /// </summary>
        /// <param name="value">Parameter value. Must not be null.</param>
        /// <returns>The parameter name, for example <c>@p0</c>.</returns>
        public string AddParameter(object value)
        {
            string name = "@p" + _Parameters.Count.ToString(CultureInfo.InvariantCulture);
            _Parameters.Add(new KeyValuePair<string, object>(name, value));
            return name;
        }

        /// <summary>
        /// Returns the parameters as JSON for diagnostics.
        /// </summary>
        /// <returns>The JSON, or null when there are no parameters.</returns>
        public string? ParametersJson()
        {
            if (_Parameters.Count == 0) return null;
            using System.IO.MemoryStream buffer = new System.IO.MemoryStream();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<string, object> parameter in _Parameters)
                {
                    writer.WritePropertyName(parameter.Key);
                    WriteValue(writer, parameter.Value);
                }

                writer.WriteEndObject();
            }

            return Encoding.UTF8.GetString(buffer.ToArray());
        }

        /// <summary>
        /// Writes a parameter value as JSON.
        /// </summary>
        /// <param name="writer">Writer. Must not be null.</param>
        /// <param name="value">Value (string, long, ulong, double, decimal, bool, or an array of those).</param>
        public static void WriteValue(Utf8JsonWriter writer, object? value)
        {
            switch (value)
            {
                case null: writer.WriteNullValue(); break;
                case string text: writer.WriteStringValue(text); break;
                case long l: writer.WriteNumberValue(l); break;
                case ulong ul: writer.WriteNumberValue(ul); break;
                case double d: writer.WriteNumberValue(d); break;
                case decimal m: writer.WriteNumberValue(m); break;
                case bool b: writer.WriteBooleanValue(b); break;
                default: writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture)); break;
            }
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitLogical(LogicalNode node)
        {
            bool topLevel = _TopLevel;
            _TopLevel = topLevel && node.Operator == LogicalOperator.And;
            CosmosDbSqlFragment? left = Visit(node.Left);
            _TopLevel = topLevel && node.Operator == LogicalOperator.And;
            CosmosDbSqlFragment? right = Visit(node.Right);
            _TopLevel = topLevel;
            if (node.Operator == LogicalOperator.Or)
            {
                if (left == null || right == null) return null;
                return new CosmosDbSqlFragment("(" + left.Text + " OR " + right.Text + ")", left.Exact && right.Exact);
            }

            if (left != null && right != null) return new CosmosDbSqlFragment("(" + left.Text + " AND " + right.Text + ")", left.Exact && right.Exact);
            if (left != null) return new CosmosDbSqlFragment(left.Text, false);
            if (right != null) return new CosmosDbSqlFragment(right.Text, false);
            return null;
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitNot(NotNode node)
        {
            bool topLevel = _TopLevel;
            _TopLevel = false;
            CosmosDbSqlFragment? operand = Visit(node.Operand);
            _TopLevel = topLevel;
            if (operand == null || !operand.Exact) return null;
            return new CosmosDbSqlFragment("(NOT " + operand.Text + ")", true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitValue(ValueNode node)
        {
            if (node.Value is bool constant) return new CosmosDbSqlFragment(constant ? "true" : "false", true);
            return null;
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitColumn(ColumnNode node)
        {
            if (!ReferenceEquals(node.Source, _Source) || _Schema.Kind(node.Column) != CosmosDbValueKind.Boolean) return null;
            string path = _Schema.Path(node.Column);
            return new CosmosDbSqlFragment("(IS_BOOL(" + path + ") AND " + path + " = true)", true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitNullCheck(NullCheckNode node)
        {
            if (!OwnColumn(node.Operand, out ColumnMetadata? column)) return null;
            return new CosmosDbSqlFragment(node.IsNull ? IsNull(_Schema.Path(column!)) : IsNotNull(_Schema.Path(column!)), true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitComparison(ComparisonNode node)
        {
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

            CosmosDbValueKind kind = _Schema.Kind(column!);
            string path = _Schema.Path(column!);
            object? stored = value.Column != null ? _Values.ToStored(value.Column, value.Value) : value.Value;
            if (stored == null)
            {
                if (op == ComparisonOperator.Equal) return new CosmosDbSqlFragment(IsNull(path), true);
                if (op == ComparisonOperator.NotEqual) return new CosmosDbSqlFragment(IsNotNull(path), true);
                return new CosmosDbSqlFragment("false", true);
            }

            if (!Compatible(stored, column!)) return null;

            bool strings = node.Left.ClrType == typeof(string) || node.Right.ClrType == typeof(string);
            if (kind == CosmosDbValueKind.String && strings && node.StringMode == StringMatchMode.IgnoreCase)
            {
                if (op != ComparisonOperator.Equal || !SafeCaseInsensitivePattern((string)CosmosDbJsonCodec.Parameter(stored))) return null;
                string name = AddParameter(CosmosDbJsonCodec.Parameter(stored));
                return new CosmosDbSqlFragment("(IS_STRING(" + path + ") AND STRINGEQUALS(" + path + ", " + name + ", true))", false);
            }

            switch (kind)
            {
                case CosmosDbValueKind.DateTime:
                    return DateTimeComparison(path, op, (DateTime)stored);
                case CosmosDbValueKind.Bytes:
                    if (op != ComparisonOperator.Equal && op != ComparisonOperator.NotEqual) return null;
                    return Direct(path, op, kind, CosmosDbJsonCodec.Parameter(stored), true);
                case CosmosDbValueKind.String:
                    if (op != ComparisonOperator.Equal && op != ComparisonOperator.NotEqual && CosmosDbJsonCodec.HasHighCharacters((string)CosmosDbJsonCodec.Parameter(stored))) return null;
                    break;
                case CosmosDbValueKind.Single:
                case CosmosDbValueKind.Double:
                    {
                        double number = Convert.ToDouble(stored, CultureInfo.InvariantCulture);
                        if (double.IsNaN(number) || double.IsInfinity(number)) return null;
                        break;
                    }
                case CosmosDbValueKind.Int64:
                    if (!SafeInteger(stored)) return Widened(path, op, kind, CosmosDbJsonCodec.Parameter(stored));
                    break;
                case CosmosDbValueKind.Decimal:
                    if (_ConservativeDecimals || !CosmosDbJsonCodec.RoundTrips((decimal)stored)) return Widened(path, op, kind, CosmosDbJsonCodec.Parameter(stored));
                    _UncertainDecimals.Add(column!);
                    break;
            }

            if (_TopLevel && op == ComparisonOperator.Equal && kind != CosmosDbValueKind.Decimal && kind != CosmosDbValueKind.Single && kind != CosmosDbValueKind.Double)
                _Equalities[column!] = stored;
            return Direct(path, op, kind, CosmosDbJsonCodec.Parameter(stored), true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitIn(InNode node)
        {
            if (!OwnColumn(node.Item, out ColumnMetadata? column)) return null;
            CosmosDbValueKind kind = _Schema.Kind(column!);
            if (kind == CosmosDbValueKind.DateTime) return null;
            if (kind == CosmosDbValueKind.String && node.Item.ClrType == typeof(string) && node.StringMode == StringMatchMode.IgnoreCase) return null;

            string path = _Schema.Path(column!);
            bool hasNull = false;
            bool exact = true;
            List<string> names = new List<string>();
            foreach (object? raw in node.Values)
            {
                object? stored = _Values.ToStored(column!, raw);
                if (stored == null)
                {
                    hasNull = true;
                    continue;
                }

                if (!Compatible(stored, column!)) return null;
                if (kind == CosmosDbValueKind.Single || kind == CosmosDbValueKind.Double)
                {
                    double number = Convert.ToDouble(stored, CultureInfo.InvariantCulture);
                    if (double.IsNaN(number) || double.IsInfinity(number)) return null;
                }
                else if (kind == CosmosDbValueKind.Int64 && !SafeInteger(stored))
                {
                    exact = false;
                }
                else if (kind == CosmosDbValueKind.Decimal)
                {
                    if (_ConservativeDecimals || !CosmosDbJsonCodec.RoundTrips((decimal)stored)) exact = false;
                    else _UncertainDecimals.Add(column!);
                }

                names.Add(AddParameter(CosmosDbJsonCodec.Parameter(stored)));
            }

            List<string> parts = new List<string>();
            if (names.Count > 0) parts.Add("(" + Guard(kind, path) + " AND " + path + " IN (" + string.Join(", ", names) + "))");
            if (hasNull) parts.Add(IsNull(path));
            string text = parts.Count == 0 ? "false" : parts.Count == 1 ? parts[0] : "(" + string.Join(" OR ", parts) + ")";
            if (!node.Negated) return new CosmosDbSqlFragment(text, exact);
            if (!exact) return null;
            return new CosmosDbSqlFragment("(NOT " + text + ")", true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitStringMatch(StringMatchNode node)
        {
            if (!OwnColumn(node.Target, out ColumnMetadata? column) || node.Pattern is not ValueNode pattern) return null;
            if (_Schema.Kind(column!) != CosmosDbValueKind.String || column!.Converter != null || node.Target.ClrType != typeof(string)) return null;
            if (pattern.Value is not string text) return null;

            string function = node.Kind == StringMatchKind.StartsWith ? "STARTSWITH" : node.Kind == StringMatchKind.EndsWith ? "ENDSWITH" : "CONTAINS";
            string path = _Schema.Path(column);
            if (node.Mode == StringMatchMode.IgnoreCase)
            {
                if (!SafeCaseInsensitivePattern(text)) return null;
                return new CosmosDbSqlFragment("(IS_STRING(" + path + ") AND " + function + "(" + path + ", " + AddParameter(text) + ", true))", false);
            }

            return new CosmosDbSqlFragment("(IS_STRING(" + path + ") AND " + function + "(" + path + ", " + AddParameter(text) + "))", true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitStringTest(StringTestNode node)
        {
            if (node.Kind != StringTestKind.IsNullOrEmpty || !OwnColumn(node.Operand, out ColumnMetadata? column)) return null;
            if (_Schema.Kind(column!) != CosmosDbValueKind.String || column!.Converter != null) return null;
            string path = _Schema.Path(column);
            return new CosmosDbSqlFragment("(NOT IS_STRING(" + path + ") OR " + path + " = \"\")", true);
        }

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitArithmetic(ArithmeticNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitNegate(NegateNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitConcat(ConcatNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitCoalesce(CoalesceNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitConditional(ConditionalNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitFunction(FunctionNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitNavigationMember(NavigationMemberNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitCollection(CollectionNode node) => null;

        /// <inheritdoc />
        public override CosmosDbSqlFragment? VisitAggregate(AggregateNode node) => null;

        /// <summary>
        /// Returns the Cosmos DB type check for a kind.
        /// </summary>
        /// <param name="kind">Kind.</param>
        /// <param name="path">Property path.</param>
        /// <returns>The check, for example <c>IS_NUMBER(c["x"])</c>.</returns>
        public static string Guard(CosmosDbValueKind kind, string path)
        {
            if (CosmosDbJsonCodec.IsNumberKind(kind)) return "IS_NUMBER(" + path + ")";
            if (kind == CosmosDbValueKind.Boolean) return "IS_BOOL(" + path + ")";
            return "IS_STRING(" + path + ")";
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

        private CosmosDbSqlFragment Direct(string path, ComparisonOperator op, CosmosDbValueKind kind, object parameter, bool exact)
        {
            string name = AddParameter(parameter);
            string guard = Guard(kind, path);
            string equal = "(" + guard + " AND " + path + " = " + name + ")";
            if (op == ComparisonOperator.Equal) return new CosmosDbSqlFragment(equal, exact);
            if (op == ComparisonOperator.NotEqual) return new CosmosDbSqlFragment("(NOT " + equal + ")", exact);

            string comparison = "(" + guard + " AND " + path + " " + Symbol(op) + " " + name + ")";
            if (kind == CosmosDbValueKind.Single || kind == CosmosDbValueKind.Double)
            {
                // NaN and the infinities are stored as strings; C# (QueryValueComparer) orders NaN below every number.
                if (op == ComparisonOperator.GreaterThan || op == ComparisonOperator.GreaterThanOrEqual)
                    comparison = "(" + comparison + " OR " + path + " = \"Infinity\")";
                else
                    comparison = "(" + comparison + " OR " + path + " = \"-Infinity\" OR " + path + " = \"NaN\")";
            }

            return new CosmosDbSqlFragment(comparison, exact);
        }

        private CosmosDbSqlFragment? Widened(string path, ComparisonOperator op, CosmosDbValueKind kind, object parameter)
        {
            switch (op)
            {
                case ComparisonOperator.Equal: return Direct(path, ComparisonOperator.Equal, kind, parameter, false);
                case ComparisonOperator.GreaterThan:
                case ComparisonOperator.GreaterThanOrEqual: return Direct(path, ComparisonOperator.GreaterThanOrEqual, kind, parameter, false);
                case ComparisonOperator.LessThan:
                case ComparisonOperator.LessThanOrEqual: return Direct(path, ComparisonOperator.LessThanOrEqual, kind, parameter, false);
                default: return null;
            }
        }

        private CosmosDbSqlFragment DateTimeComparison(string path, ComparisonOperator op, DateTime value)
        {
            string prefix = CosmosDbJsonCodec.DateTimePrefix(value);
            string guard = "IS_STRING(" + path + ")";
            string low = AddParameter(prefix);
            string high = AddParameter(prefix + CosmosDbJsonCodec.PrefixUpperBound);
            string equal = "(" + guard + " AND " + path + " >= " + low + " AND " + path + " < " + high + ")";
            switch (op)
            {
                case ComparisonOperator.Equal: return new CosmosDbSqlFragment(equal, true);
                case ComparisonOperator.NotEqual: return new CosmosDbSqlFragment("(NOT " + equal + ")", true);
                case ComparisonOperator.GreaterThan: return new CosmosDbSqlFragment("(" + guard + " AND " + path + " >= " + high + ")", true);
                case ComparisonOperator.GreaterThanOrEqual: return new CosmosDbSqlFragment("(" + guard + " AND " + path + " >= " + low + ")", true);
                case ComparisonOperator.LessThan: return new CosmosDbSqlFragment("(" + guard + " AND " + path + " < " + low + ")", true);
                default: return new CosmosDbSqlFragment("(" + guard + " AND " + path + " < " + high + ")", true);
            }
        }

        private bool Compatible(object stored, ColumnMetadata column)
        {
            Type expected = _Schema.StoredType(column);
            Type actual = stored.GetType();
            if (actual == expected) return true;
            CosmosDbValueKind kind = _Schema.Kind(column);
            if (kind != CosmosDbValueKind.Integer && kind != CosmosDbValueKind.Int64) return false;
            if (expected == typeof(TimeSpan) || expected == typeof(TimeOnly) || actual == typeof(TimeSpan) || actual == typeof(TimeOnly)) return false;
            return actual == typeof(int) || actual == typeof(long) || actual == typeof(short) || actual == typeof(byte)
                || actual == typeof(sbyte) || actual == typeof(ushort) || actual == typeof(uint) || actual == typeof(ulong);
        }

        private static bool SafeInteger(object stored)
        {
            switch (stored)
            {
                case long l: return l <= CosmosDbJsonCodec.MaxExactInteger && l >= -CosmosDbJsonCodec.MaxExactInteger;
                case ulong ul: return ul <= (ulong)CosmosDbJsonCodec.MaxExactInteger;
                case TimeSpan span: return span.Ticks <= CosmosDbJsonCodec.MaxExactInteger && span.Ticks >= -CosmosDbJsonCodec.MaxExactInteger;
                default: return true;
            }
        }

        private static bool SafeCaseInsensitivePattern(string pattern)
        {
            foreach (char c in pattern)
            {
                if (c >= 0x80 || c == 'i' || c == 'I' || c == 's' || c == 'S') return false;
            }

            return true;
        }

        private static string IsNull(string path)
        {
            return "(NOT IS_DEFINED(" + path + ") OR IS_NULL(" + path + "))";
        }

        private static string IsNotNull(string path)
        {
            return "(IS_DEFINED(" + path + ") AND NOT IS_NULL(" + path + "))";
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

        private static string Symbol(ComparisonOperator op)
        {
            switch (op)
            {
                case ComparisonOperator.LessThan: return "<";
                case ComparisonOperator.LessThanOrEqual: return "<=";
                case ComparisonOperator.GreaterThan: return ">";
                case ComparisonOperator.GreaterThanOrEqual: return ">=";
                case ComparisonOperator.NotEqual: return "!=";
                default: return "=";
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
