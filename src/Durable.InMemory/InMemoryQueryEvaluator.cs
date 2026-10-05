namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees against stored rows of an <see cref="InMemoryDatabaseState"/> with C#
    /// semantics: <c>null == null</c> is true, a comparison or string match involving null is false (so its negation is
    /// true), arithmetic and functions on null yield null, <c>!list.Contains(x)</c> is true for a null item when the list
    /// has no null. String comparisons follow the node's <see cref="StringMatchMode"/>: <see cref="StringMatchMode.Database"/>
    /// and <see cref="StringMatchMode.Ordinal"/> are ordinal, <see cref="StringMatchMode.IgnoreCase"/> is
    /// <see cref="StringComparison.OrdinalIgnoreCase"/>. Values are compared in their stored form (enum names, converter
    /// provider values, JSON text), exactly as a database compares column values with converted parameters.
    /// Navigation members look up the related row by key; collection Any/All/Count look up related rows (through the
    /// junction table for many-to-many) and exclude soft-deleted related rows, as the SQL engine does. Functions follow SQL
    /// where C# would throw: Substring clamps out-of-range arguments, Replace with an empty search string returns the input,
    /// and Math.Round rounds midpoints away from zero (SQL ROUND).
    /// Thread safety: not thread-safe; create one per operation. The state it reads is immutable.
    /// </summary>
    internal sealed class InMemoryQueryEvaluator : QueryNodeVisitor<object?>
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the rows of the current group, for <see cref="AggregateNode"/>s; null outside grouping.
        /// </summary>
        public IReadOnlyList<InMemoryRow>? GroupRows { get; set; }

        /// <summary>
        /// Gets or sets the source the group rows are bound to; null outside grouping.
        /// </summary>
        public QuerySource? GroupSource { get; set; }

        #endregion

        #region Private-Members

        private static readonly object _True = true;
        private static readonly object _False = false;
        private readonly InMemoryDatabaseState _State;
        private readonly InMemoryValueConverter _Values;
        private readonly Dictionary<QuerySource, InMemoryRow> _Bindings = new Dictionary<QuerySource, InMemoryRow>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an evaluator.
        /// </summary>
        /// <param name="state">State related rows are read from. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public InMemoryQueryEvaluator(InMemoryDatabaseState state, InMemoryValueConverter values)
        {
            _State = state ?? throw new ArgumentNullException(nameof(state));
            _Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Binds a source to a row.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="row">Row. Must not be null.</param>
        /// <returns>This evaluator.</returns>
        public InMemoryQueryEvaluator Bind(QuerySource source, InMemoryRow row)
        {
            _Bindings[source] = row;
            return this;
        }

        /// <summary>
        /// Evaluates a node as a condition; anything other than true (including null) is false.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The truth value.</returns>
        public bool Test(QueryNode node)
        {
            return Visit(node) is bool value && value;
        }

        /// <summary>
        /// Returns whether a stored row is soft-deleted.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="row">Row. Must not be null.</param>
        /// <returns>True when the soft-delete marker is set.</returns>
        public static bool IsSoftDeleted(EntityMetadata metadata, InMemoryRow row)
        {
            ColumnMetadata? column = metadata.SoftDeleteColumn;
            if (column == null) return false;
            object? marker = row.Values[InMemoryTableSchema.For(metadata).Ordinal(column)];
            if (column.ClrType == typeof(bool)) return marker is bool flag && flag;
            return marker != null;
        }

        /// <inheritdoc />
        public override object? VisitColumn(ColumnNode node)
        {
            if (!_Bindings.TryGetValue(node.Source, out InMemoryRow? row))
                throw new InvalidOperationException("Query source '" + node.Source + "' is not bound to a row.");
            return row.Values[InMemoryTableSchema.For(node.Source.Metadata).Ordinal(node.Column)];
        }

        /// <inheritdoc />
        public override object? VisitValue(ValueNode node)
        {
            return node.Column != null ? _Values.ToStored(node.Column, node.Value) : node.Value;
        }

        /// <inheritdoc />
        public override object? VisitComparison(ComparisonNode node)
        {
            object? left = Visit(node.Left);
            object? right = Visit(node.Right);
            bool strings = node.Left.ClrType == typeof(string) || node.Right.ClrType == typeof(string);
            QueryValueComparer comparer = QueryValueComparer.For(strings ? node.StringMode : StringMatchMode.Database);
            switch (node.Operator)
            {
                case ComparisonOperator.Equal:
                    return Box(comparer.Equals(left, right));
                case ComparisonOperator.NotEqual:
                    return Box(!comparer.Equals(left, right));
            }

            if (left == null || right == null) return _False;
            int comparison = comparer.Compare(left, right);
            switch (node.Operator)
            {
                case ComparisonOperator.LessThan: return Box(comparison < 0);
                case ComparisonOperator.LessThanOrEqual: return Box(comparison <= 0);
                case ComparisonOperator.GreaterThan: return Box(comparison > 0);
                default: return Box(comparison >= 0);
            }
        }

        /// <inheritdoc />
        public override object? VisitLogical(LogicalNode node)
        {
            if (node.Operator == LogicalOperator.And) return Box(Test(node.Left) && Test(node.Right));
            return Box(Test(node.Left) || Test(node.Right));
        }

        /// <inheritdoc />
        public override object? VisitNot(NotNode node)
        {
            return Box(!Test(node.Operand));
        }

        /// <inheritdoc />
        public override object? VisitNullCheck(NullCheckNode node)
        {
            return Box((Visit(node.Operand) == null) == node.IsNull);
        }

        /// <inheritdoc />
        public override object? VisitIn(InNode node)
        {
            object? item = Visit(node.Item);
            ColumnMetadata? column = (node.Item as ColumnNode)?.Column;
            QueryValueComparer comparer = QueryValueComparer.For(node.Item.ClrType == typeof(string) ? node.StringMode : StringMatchMode.Database);
            bool hasNull = false;
            bool found = false;
            foreach (object? raw in node.Values)
            {
                object? value = column != null ? _Values.ToStored(column, raw) : raw;
                if (value == null)
                {
                    hasNull = true;
                    continue;
                }

                if (item != null && comparer.Equals(item, value))
                {
                    found = true;
                    break;
                }
            }

            if (item == null) found = hasNull;
            return Box(node.Negated ? !found : found);
        }

        /// <inheritdoc />
        public override object? VisitStringMatch(StringMatchNode node)
        {
            string? target = Text(Visit(node.Target));
            string? pattern = Text(Visit(node.Pattern));
            if (target == null || pattern == null) return _False;
            StringComparison comparison = QueryValueComparer.StringComparisonFor(node.Mode);
            switch (node.Kind)
            {
                case StringMatchKind.Contains: return Box(target.Contains(pattern, comparison));
                case StringMatchKind.StartsWith: return Box(target.StartsWith(pattern, comparison));
                default: return Box(target.EndsWith(pattern, comparison));
            }
        }

        /// <inheritdoc />
        public override object? VisitStringTest(StringTestNode node)
        {
            string? text = Text(Visit(node.Operand));
            return Box(node.Kind == StringTestKind.IsNullOrEmpty ? string.IsNullOrEmpty(text) : string.IsNullOrWhiteSpace(text));
        }

        /// <inheritdoc />
        public override object? VisitArithmetic(ArithmeticNode node)
        {
            object? left = Visit(node.Left);
            object? right = Visit(node.Right);
            if (left == null || right == null) return null;
            if (left is Enum) left = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            if (right is Enum) right = Convert.ToInt64(right, CultureInfo.InvariantCulture);

            if (left is DateTime || left is DateTimeOffset || left is DateOnly || left is TimeSpan || right is TimeSpan)
                return Temporal(node.Operator, left, right);
            if (node.Operator == ArithmeticOperator.Add && (left is string || right is string))
                return Text(left) + Text(right);

            Type resultType = Nullable.GetUnderlyingType(node.ClrType) ?? node.ClrType;
            bool floating = left is double || left is float || right is double || right is float;
            bool exact = left is decimal || right is decimal || left is ulong || right is ulong;
            if (!floating && !exact && node.Operator == ArithmeticOperator.Divide && !node.IntegerDivision)
            {
                if (resultType == typeof(decimal)) exact = true;
                else floating = true;
            }

            if (floating)
            {
                double a = Convert.ToDouble(left, CultureInfo.InvariantCulture);
                double b = Convert.ToDouble(right, CultureInfo.InvariantCulture);
                switch (node.Operator)
                {
                    case ArithmeticOperator.Add: return a + b;
                    case ArithmeticOperator.Subtract: return a - b;
                    case ArithmeticOperator.Multiply: return a * b;
                    case ArithmeticOperator.Divide: return a / b;
                    default: return a % b;
                }
            }

            if (exact)
            {
                decimal a = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
                decimal b = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
                switch (node.Operator)
                {
                    case ArithmeticOperator.Add: return a + b;
                    case ArithmeticOperator.Subtract: return a - b;
                    case ArithmeticOperator.Multiply: return a * b;
                    case ArithmeticOperator.Divide: return node.IntegerDivision ? decimal.Truncate(a / b) : a / b;
                    default: return a % b;
                }
            }

            long x = Convert.ToInt64(left, CultureInfo.InvariantCulture);
            long y = Convert.ToInt64(right, CultureInfo.InvariantCulture);
            switch (node.Operator)
            {
                case ArithmeticOperator.Add: return unchecked(x + y);
                case ArithmeticOperator.Subtract: return unchecked(x - y);
                case ArithmeticOperator.Multiply: return unchecked(x * y);
                case ArithmeticOperator.Divide: return x / y;
                default: return x % y;
            }
        }

        /// <inheritdoc />
        public override object? VisitNegate(NegateNode node)
        {
            object? value = Visit(node.Operand);
            switch (value)
            {
                case null: return null;
                case double d: return -d;
                case float f: return -f;
                case decimal m: return -m;
                case TimeSpan span: return span.Negate();
                case Enum e: return -Convert.ToInt64(e, CultureInfo.InvariantCulture);
                default: return -Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }
        }

        /// <inheritdoc />
        public override object? VisitConcat(ConcatNode node)
        {
            return string.Concat(node.Parts.Select(part => Text(Visit(part)) ?? string.Empty));
        }

        /// <inheritdoc />
        public override object? VisitCoalesce(CoalesceNode node)
        {
            return Visit(node.Left) ?? Visit(node.Right);
        }

        /// <inheritdoc />
        public override object? VisitConditional(ConditionalNode node)
        {
            return Test(node.Test) ? Visit(node.IfTrue) : Visit(node.IfFalse);
        }

        /// <inheritdoc />
        public override object? VisitFunction(FunctionNode node)
        {
            object?[] arguments = node.Arguments.Select(Visit).ToArray();
            if (arguments.Length > 0 && arguments[0] == null) return null;
            object first = arguments[0]!;

            switch (node.Function)
            {
                case QueryFunction.Length: return Text(first)!.Length;
                case QueryFunction.Upper: return Text(first)!.ToUpperInvariant();
                case QueryFunction.Lower: return Text(first)!.ToLowerInvariant();
                case QueryFunction.Trim: return Text(first)!.Trim();
                case QueryFunction.TrimStart: return Text(first)!.TrimStart();
                case QueryFunction.TrimEnd: return Text(first)!.TrimEnd();
                case QueryFunction.Substring: return Substring(Text(first)!, arguments);
                case QueryFunction.Replace:
                    {
                        if (arguments[1] == null) return null;
                        string search = Text(arguments[1])!;
                        if (search.Length == 0) return Text(first);
                        return Text(first)!.Replace(search, Text(arguments[2]) ?? string.Empty, StringComparison.Ordinal);
                    }
                case QueryFunction.IndexOf:
                    {
                        if (arguments[1] == null) return null;
                        return Text(first)!.IndexOf(Text(arguments[1])!, QueryValueComparer.StringComparisonFor(node.StringMode));
                    }
                case QueryFunction.Abs: return Abs(first);
                case QueryFunction.Round: return Round(first, arguments.Length > 1 ? arguments[1] : null);
                case QueryFunction.Ceiling: return first is decimal cm ? Math.Ceiling(cm) : Math.Ceiling(Convert.ToDouble(first, CultureInfo.InvariantCulture));
                case QueryFunction.Floor: return first is decimal fm ? Math.Floor(fm) : Math.Floor(Convert.ToDouble(first, CultureInfo.InvariantCulture));
                case QueryFunction.Power:
                    if (arguments[1] == null) return null;
                    return Math.Pow(Convert.ToDouble(first, CultureInfo.InvariantCulture), Convert.ToDouble(arguments[1], CultureInfo.InvariantCulture));
                case QueryFunction.Sqrt: return Math.Sqrt(Convert.ToDouble(first, CultureInfo.InvariantCulture));
                case QueryFunction.Year: return DatePart(first).Year;
                case QueryFunction.Month: return DatePart(first).Month;
                case QueryFunction.Day: return DatePart(first).Day;
                case QueryFunction.Hour: return DatePart(first).Hour;
                case QueryFunction.Minute: return DatePart(first).Minute;
                case QueryFunction.Second: return DatePart(first).Second;
                case QueryFunction.DayOfYear: return DatePart(first).DayOfYear;
                case QueryFunction.DayOfWeek: return DatePart(first).DayOfWeek;
                case QueryFunction.Date: return first is DateOnly dateOnly ? dateOnly : (object)DatePart(first).Date;
                default:
                    if (arguments[1] == null) return null;
                    return AddToDate(node.Function, first, arguments[1]!);
            }
        }

        /// <inheritdoc />
        public override object? VisitNavigationMember(NavigationMemberNode node)
        {
            object? key = Visit(node.OwnerKey);
            if (key == null) return null;
            EntityMetadata related = node.RelatedSource.Metadata;
            InMemoryRow? row = FindRows(related, node.Navigation.RemoteColumn, key).FirstOrDefault(r => !IsSoftDeleted(related, r));
            return row?.Values[InMemoryTableSchema.For(related).Ordinal(node.Column)];
        }

        /// <inheritdoc />
        public override object? VisitCollection(CollectionNode node)
        {
            object? key = Visit(node.OwnerKey);
            List<InMemoryRow> related = key == null ? new List<InMemoryRow>() : RelatedRows(node.Navigation, node.RelatedSource.Metadata, key);

            int matches = 0;
            bool all = true;
            InMemoryRow? previous = _Bindings.TryGetValue(node.RelatedSource, out InMemoryRow? bound) ? bound : null;
            try
            {
                foreach (InMemoryRow row in related)
                {
                    bool match = true;
                    if (node.Predicate != null)
                    {
                        _Bindings[node.RelatedSource] = row;
                        match = Test(node.Predicate);
                    }

                    if (match) matches++;
                    else all = false;
                    if (node.Operation == CollectionOperation.Any && matches > 0) break;
                    if (node.Operation == CollectionOperation.All && !all) break;
                }
            }
            finally
            {
                if (previous != null) _Bindings[node.RelatedSource] = previous;
                else _Bindings.Remove(node.RelatedSource);
            }

            switch (node.Operation)
            {
                case CollectionOperation.Any: return Box(matches > 0);
                case CollectionOperation.All: return Box(all);
                default: return CountResult(node.ClrType, matches);
            }
        }

        /// <inheritdoc />
        public override object? VisitAggregate(AggregateNode node)
        {
            IReadOnlyList<InMemoryRow> rows = GroupRows ?? throw new InvalidOperationException("Aggregate " + node.Function + " requires a group.");
            QuerySource source = GroupSource ?? throw new InvalidOperationException("Aggregate " + node.Function + " requires a group source.");
            InMemoryRow? previous = _Bindings.TryGetValue(source, out InMemoryRow? bound) ? bound : null;
            try
            {
                switch (node.Function)
                {
                    case AggregateFunction.Count:
                    case AggregateFunction.Any:
                        {
                            int count = 0;
                            foreach (InMemoryRow row in rows)
                            {
                                if (node.Operand == null || Bind(source, row).Test(node.Operand)) count++;
                            }

                            return node.Function == AggregateFunction.Any ? Box(count > 0) : CountResult(node.ClrType, count);
                        }
                    default:
                        {
                            List<object?> values = new List<object?>(rows.Count);
                            foreach (InMemoryRow row in rows) values.Add(Bind(source, row).Visit(node.Operand!));
                            object? result = QueryAggregates.Compute(node.Function, values, QueryValueComparer.Ordinal);
                            return node.Function == AggregateFunction.Sum ? result ?? 0m : result;
                        }
                }
            }
            finally
            {
                if (previous != null) _Bindings[source] = previous;
                else _Bindings.Remove(source);
            }
        }

        #endregion

        #region Private-Methods

        private static object Box(bool value)
        {
            return value ? _True : _False;
        }

        private static object CountResult(Type type, int count)
        {
            Type t = Nullable.GetUnderlyingType(type) ?? type;
            return t == typeof(long) ? (object)(long)count : count;
        }

        private static string? Text(object? value)
        {
            switch (value)
            {
                case null: return null;
                case string text: return text;
                case char c: return c.ToString();
                case IFormattable formattable: return formattable.ToString(null, CultureInfo.InvariantCulture);
                default: return value.ToString();
            }
        }

        private List<InMemoryRow> RelatedRows(NavigationMetadata navigation, EntityMetadata related, object key)
        {
            List<InMemoryRow> rows = new List<InMemoryRow>();
            if (navigation.Kind == NavigationKind.ManyToMany)
            {
                EntityMetadata junction = EntityMetadata.For(navigation.JunctionType!);
                int remoteOrdinal = InMemoryTableSchema.For(junction).Ordinal(navigation.JunctionRemoteColumn!);
                foreach (InMemoryRow link in FindRows(junction, navigation.JunctionLocalColumn!, key))
                {
                    object? remote = link.Values[remoteOrdinal];
                    if (remote == null) continue;
                    rows.AddRange(FindRows(related, navigation.RemoteColumn, remote));
                }
            }
            else
            {
                rows.AddRange(FindRows(related, navigation.RemoteColumn, key));
            }

            rows.RemoveAll(row => IsSoftDeleted(related, row));
            return rows;
        }

        private IEnumerable<InMemoryRow> FindRows(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            InMemoryTable table = _State.Table(metadata);
            if (metadata.KeyColumns.Count == 1 && ReferenceEquals(metadata.KeyColumns[0], column))
            {
                InMemoryRow? row = table.Find(new InMemoryRowKey(new[] { key }));
                return row == null ? Array.Empty<InMemoryRow>() : new[] { row };
            }

            int ordinal = InMemoryTableSchema.For(metadata).Ordinal(column);
            return table.Rows.Where(row => QueryValueComparer.Ordinal.Equals(row.Values[ordinal], key));
        }

        private static string Substring(string text, object?[] arguments)
        {
            if (arguments[1] == null) return text;
            int start = Math.Max(0, Convert.ToInt32(arguments[1], CultureInfo.InvariantCulture));
            if (start >= text.Length) return string.Empty;
            if (arguments.Length < 3 || arguments[2] == null) return text.Substring(start);
            int length = Math.Max(0, Convert.ToInt32(arguments[2], CultureInfo.InvariantCulture));
            return text.Substring(start, Math.Min(length, text.Length - start));
        }

        private static object Abs(object value)
        {
            switch (value)
            {
                case decimal m: return Math.Abs(m);
                case double d: return Math.Abs(d);
                case float f: return Math.Abs(f);
                default: return Math.Abs(Convert.ToInt64(value, CultureInfo.InvariantCulture));
            }
        }

        private static object Round(object value, object? digits)
        {
            int places = digits == null ? 0 : Convert.ToInt32(digits, CultureInfo.InvariantCulture);
            if (value is decimal m) return Math.Round(m, places, MidpointRounding.AwayFromZero);
            if (value is double || value is float) return Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), places, MidpointRounding.AwayFromZero);
            return value;
        }

        private static DateTime DatePart(object value)
        {
            switch (value)
            {
                case DateTime dateTime: return dateTime;
                case DateTimeOffset offset: return offset.DateTime;
                case DateOnly date: return date.ToDateTime(TimeOnly.MinValue);
                default: return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
            }
        }

        private static object AddToDate(QueryFunction function, object value, object amount)
        {
            double count = Convert.ToDouble(amount, CultureInfo.InvariantCulture);
            int whole = (int)count;
            if (value is DateOnly date)
            {
                switch (function)
                {
                    case QueryFunction.AddYears: return date.AddYears(whole);
                    case QueryFunction.AddMonths: return date.AddMonths(whole);
                    case QueryFunction.AddDays: return date.AddDays(whole);
                    default: throw new NotSupportedException(function + " is not supported on DateOnly.");
                }
            }

            if (value is DateTimeOffset offset)
            {
                switch (function)
                {
                    case QueryFunction.AddYears: return offset.AddYears(whole);
                    case QueryFunction.AddMonths: return offset.AddMonths(whole);
                    case QueryFunction.AddDays: return offset.AddDays(count);
                    case QueryFunction.AddHours: return offset.AddHours(count);
                    case QueryFunction.AddMinutes: return offset.AddMinutes(count);
                    default: return offset.AddSeconds(count);
                }
            }

            DateTime dateTime = DatePart(value);
            switch (function)
            {
                case QueryFunction.AddYears: return dateTime.AddYears(whole);
                case QueryFunction.AddMonths: return dateTime.AddMonths(whole);
                case QueryFunction.AddDays: return dateTime.AddDays(count);
                case QueryFunction.AddHours: return dateTime.AddHours(count);
                case QueryFunction.AddMinutes: return dateTime.AddMinutes(count);
                case QueryFunction.AddSeconds: return dateTime.AddSeconds(count);
                default: throw new NotSupportedException("Function " + function + " is not supported.");
            }
        }

        private static object Temporal(ArithmeticOperator op, object left, object right)
        {
            if (left is DateTime a && right is DateTime b && op == ArithmeticOperator.Subtract) return a - b;
            if (left is DateTimeOffset c && right is DateTimeOffset d && op == ArithmeticOperator.Subtract) return c - d;
            if (left is DateTime e && right is TimeSpan f) return op == ArithmeticOperator.Add ? e + f : op == ArithmeticOperator.Subtract ? e - f : throw UnsupportedTemporal(op);
            if (left is DateTimeOffset g && right is TimeSpan h) return op == ArithmeticOperator.Add ? g + h : op == ArithmeticOperator.Subtract ? g - h : throw UnsupportedTemporal(op);
            if (left is TimeSpan i && right is TimeSpan j) return op == ArithmeticOperator.Add ? i + j : op == ArithmeticOperator.Subtract ? i - j : throw UnsupportedTemporal(op);
            if (left is TimeSpan k && right is DateTime l && op == ArithmeticOperator.Add) return l + k;
            throw UnsupportedTemporal(op);
        }

        private static NotSupportedException UnsupportedTemporal(ArithmeticOperator op)
        {
            return new NotSupportedException("Operator " + op + " is not supported for these date/time operands.");
        }

        #endregion
    }
}
