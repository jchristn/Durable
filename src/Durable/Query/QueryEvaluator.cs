namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Durable;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees client-side against a backend's own row representation, with C# semantics,
    /// and applies a <see cref="QueryModel"/> (filter, ordering, paging) to a sequence of rows. Non-SQL backends use it for
    /// everything they cannot push down to their store: residual predicates, navigation members, collection predicates,
    /// computed values, ordering and grouping aggregates. The in-memory backend evaluates every query with it.
    /// <para>
    /// Semantics: <c>null == null</c> is true; a comparison or string match involving null is false (so its negation is
    /// true); arithmetic and functions on null yield null; <c>!list.Contains(x)</c> is true for a null item when the list
    /// has no null. String comparisons follow the node's <see cref="StringMatchMode"/>: <see cref="StringMatchMode.Database"/>
    /// and <see cref="StringMatchMode.Ordinal"/> are ordinal, <see cref="StringMatchMode.IgnoreCase"/> is
    /// <see cref="StringComparison.OrdinalIgnoreCase"/>. Values are compared in the form <see cref="GetValue"/> returns, and
    /// client-side parameter values are first converted to that form with <see cref="NormalizeValue"/>, exactly as a
    /// database compares column values with converted parameters. Navigation members look up the related row by key;
    /// collection Any/All/Count look up related rows (through the junction entity for many-to-many) and exclude
    /// soft-deleted related rows, as the SQL engine does. Functions follow SQL where C# would throw: Substring clamps
    /// out-of-range arguments, Replace with an empty search string returns the input, and Math.Round rounds midpoints away
    /// from zero (SQL ROUND). Ordering (<see cref="Sort"/>) is stable, nulls first, ordinal for strings.
    /// </para>
    /// <para>
    /// Using it from a backend: subclass it for your row type and implement <see cref="GetValue"/> (read a column of a
    /// row) and <see cref="FindRows"/> (rows of an entity whose column equals a key; used for navigations and junction
    /// entities). Override <see cref="NormalizeValue"/> when stored values differ from model values (enum names,
    /// converter provider values, JSON text, ...). Then, per operation, create an evaluator and either call
    /// <see cref="Apply"/> / <see cref="Filter"/> / <see cref="Sort"/> / <see cref="Aggregate"/> over candidate rows, or
    /// <see cref="Bind"/> the query's <see cref="QuerySource"/> to a row and call <see cref="Test"/> (conditions) or
    /// <see cref="QueryNodeVisitor{TResult}.Visit"/> (values). For grouping aggregates, set <see cref="GroupSource"/> and
    /// <see cref="GroupRows"/> to the group being evaluated.
    /// </para>
    /// Thread safety: not thread-safe (it holds the current row bindings); create one per evaluation. Subclasses must
    /// only read storage that does not change during the evaluation (for example a snapshot).
    /// </summary>
    /// <typeparam name="TRow">The backend's row (document, record, node) type.</typeparam>
    public abstract class QueryEvaluator<TRow> : QueryNodeVisitor<object?>
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the rows of the current group, for <see cref="AggregateNode"/>s; null outside grouping.
        /// Default: null.
        /// </summary>
        public IReadOnlyList<TRow>? GroupRows { get; set; }

        /// <summary>
        /// Gets or sets the source the group rows are bound to while an aggregate is evaluated; null outside grouping.
        /// Default: null.
        /// </summary>
        public QuerySource? GroupSource { get; set; }

        #endregion

        #region Private-Members

        private static readonly object _True = true;
        private static readonly object _False = false;
        private readonly Dictionary<QuerySource, TRow> _Bindings = new Dictionary<QuerySource, TRow>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an evaluator with no bindings.
        /// </summary>
        protected QueryEvaluator()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Binds a source to a row, so <see cref="ColumnNode"/>s of that source read the row.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="row">Row. Must not be null.</param>
        /// <returns>This evaluator, for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public QueryEvaluator<TRow> Bind(QuerySource source, TRow row)
        {
            ArgumentNullException.ThrowIfNull(source);
            if (row == null) throw new ArgumentNullException(nameof(row));
            _Bindings[source] = row;
            return this;
        }

        /// <summary>
        /// Evaluates a node as a condition against the bound rows; anything other than true (including null) is false.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The truth value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when node is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when a referenced source is not bound.</exception>
        public bool Test(QueryNode node)
        {
            return Visit(node) is bool value && value;
        }

        /// <summary>
        /// Returns the rows that satisfy a filter, in their original order.
        /// </summary>
        /// <param name="source">Source the filter's columns refer to. Must not be null.</param>
        /// <param name="filter">Filter; null keeps every row.</param>
        /// <param name="rows">Candidate rows. Must not be null.</param>
        /// <returns>A new list of the matching rows. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when source or rows is null.</exception>
        public List<TRow> Filter(QuerySource source, QueryNode? filter, IEnumerable<TRow> rows)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(rows);
            if (filter == null) return rows.ToList();
            return rows.Where(row => Bind(source, row).Test(filter)).ToList();
        }

        /// <summary>
        /// Orders rows by orderings: stable (ties keep their input order), nulls first, ordinal for strings
        /// (<see cref="QueryValueComparer.Ordinal"/>). Each key is evaluated once per row.
        /// </summary>
        /// <param name="source">Source the ordering keys refer to. Must not be null.</param>
        /// <param name="orderings">Orderings, primary first. Must not be null; empty keeps the input order.</param>
        /// <param name="rows">Rows. Must not be null.</param>
        /// <returns>A new list of the ordered rows. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public List<TRow> Sort(QuerySource source, IReadOnlyList<QueryOrdering> orderings, IEnumerable<TRow> rows)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(orderings);
            ArgumentNullException.ThrowIfNull(rows);
            List<TRow> list = rows.ToList();
            if (orderings.Count == 0) return list;

            object?[][] keys = new object?[list.Count][];
            for (int r = 0; r < list.Count; r++)
            {
                Bind(source, list[r]);
                object?[] values = new object?[orderings.Count];
                for (int i = 0; i < values.Length; i++) values[i] = Visit(orderings[i].Key);
                keys[r] = values;
            }

            IEnumerable<int> positions = Enumerable.Range(0, list.Count);
            IOrderedEnumerable<int>? ordered = null;
            for (int i = 0; i < orderings.Count; i++)
            {
                int index = i;
                bool descending = orderings[i].Descending;
                Func<int, object?> selector = position => keys[position][index];
                if (ordered == null)
                    ordered = descending ? positions.OrderByDescending(selector, QueryValueComparer.Ordinal) : positions.OrderBy(selector, QueryValueComparer.Ordinal);
                else
                    ordered = descending ? ordered.ThenByDescending(selector, QueryValueComparer.Ordinal) : ordered.ThenBy(selector, QueryValueComparer.Ordinal);
            }

            List<TRow> result = new List<TRow>(list.Count);
            foreach (int position in ordered!) result.Add(list[position]);
            return result;
        }

        /// <summary>
        /// Applies a query model to rows: <see cref="QueryModel.Filter"/>, then <see cref="QueryModel.Orderings"/>
        /// (see <see cref="Sort"/>), then <see cref="QueryModel.Skip"/> and <see cref="QueryModel.Take"/>. The model's
        /// transaction and <see cref="QueryModel.Distinct"/> are not considered (rows of one entity are distinct by key).
        /// </summary>
        /// <param name="model">Query model. Must not be null.</param>
        /// <param name="rows">Candidate rows of the model's entity. Must not be null.</param>
        /// <returns>A new list of the resulting rows. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public List<TRow> Apply(QueryModel model, IEnumerable<TRow> rows)
        {
            ArgumentNullException.ThrowIfNull(model);
            ArgumentNullException.ThrowIfNull(rows);
            List<TRow> result = Filter(model.Source, model.Filter, rows);
            if (model.Orderings.Count > 0) result = Sort(model.Source, model.Orderings, result);

            IEnumerable<TRow> paged = result;
            if (model.Skip.HasValue) paged = paged.Skip(model.Skip.Value);
            if (model.Take.HasValue) paged = paged.Take(model.Take.Value);
            return paged is List<TRow> list ? list : paged.ToList();
        }

        /// <summary>
        /// Computes an aggregate of an operand over rows with SQL semantics (see <see cref="QueryAggregates"/>): nulls are
        /// ignored and an aggregate over no non-null values is null. The result is in the evaluator's value form (for Min
        /// and Max of a column, the stored form; convert it back to the model type when needed).
        /// </summary>
        /// <param name="source">Source the operand refers to. Must not be null.</param>
        /// <param name="rows">Rows. Must not be null.</param>
        /// <param name="function">Sum, Average, Min or Max.</param>
        /// <param name="operand">Operand. Must not be null.</param>
        /// <returns>The aggregate, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when <paramref name="function"/> is Count or Any.</exception>
        public object? Aggregate(QuerySource source, IEnumerable<TRow> rows, AggregateFunction function, QueryNode operand)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(rows);
            ArgumentNullException.ThrowIfNull(operand);
            List<object?> values = new List<object?>();
            foreach (TRow row in rows) values.Add(Bind(source, row).Visit(operand));
            return QueryAggregates.Compute(function, values, QueryValueComparer.Ordinal);
        }

        /// <summary>
        /// Returns whether a row is soft-deleted: its <see cref="EntityMetadata.SoftDeleteColumn"/> is true (bool
        /// columns) or not null (other columns). Always false for entities without a soft-delete column.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="row">Row of that entity. Must not be null.</param>
        /// <returns>True when the soft-delete marker is set.</returns>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public virtual bool IsSoftDeleted(EntityMetadata metadata, TRow row)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ColumnMetadata? column = metadata.SoftDeleteColumn;
            if (column == null) return false;
            object? marker = GetValue(metadata, row, column);
            if (column.ClrType == typeof(bool)) return marker is bool flag && flag;
            return marker != null;
        }

        /// <inheritdoc />
        public override object? VisitColumn(ColumnNode node)
        {
            if (!_Bindings.TryGetValue(node.Source, out TRow? row))
                throw new InvalidOperationException("Query source '" + node.Source + "' is not bound to a row.");
            return GetValue(node.Source.Metadata, row!, node.Column);
        }

        /// <inheritdoc />
        public override object? VisitValue(ValueNode node)
        {
            return node.Column != null ? NormalizeValue(node.Column, node.Value) : node.Value;
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
                object? value = column != null ? NormalizeValue(column, raw) : raw;
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
            foreach (TRow row in FindRows(related, node.Navigation.RemoteColumn, key))
            {
                if (!IsSoftDeleted(related, row)) return GetValue(related, row, node.Column);
            }

            return null;
        }

        /// <inheritdoc />
        public override object? VisitCollection(CollectionNode node)
        {
            object? key = Visit(node.OwnerKey);
            List<TRow> related = key == null ? new List<TRow>() : RelatedRows(node.Navigation, node.RelatedSource.Metadata, key);

            int matches = 0;
            bool all = true;
            bool hadPrevious = _Bindings.TryGetValue(node.RelatedSource, out TRow? previous);
            try
            {
                foreach (TRow row in related)
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
                Restore(node.RelatedSource, hadPrevious, previous);
            }

            switch (node.Operation)
            {
                case CollectionOperation.Any: return Box(matches > 0);
                case CollectionOperation.All: return Box(all);
                default: return CountResult(node.ClrType, matches);
            }
        }

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">Thrown when <see cref="GroupRows"/> or <see cref="GroupSource"/> is not set.</exception>
        public override object? VisitAggregate(AggregateNode node)
        {
            IReadOnlyList<TRow> rows = GroupRows ?? throw new InvalidOperationException("Aggregate " + node.Function + " requires a group.");
            QuerySource source = GroupSource ?? throw new InvalidOperationException("Aggregate " + node.Function + " requires a group source.");
            bool hadPrevious = _Bindings.TryGetValue(source, out TRow? previous);
            try
            {
                switch (node.Function)
                {
                    case AggregateFunction.Count:
                    case AggregateFunction.Any:
                        {
                            int count = 0;
                            foreach (TRow row in rows)
                            {
                                if (node.Operand == null || Bind(source, row).Test(node.Operand)) count++;
                            }

                            return node.Function == AggregateFunction.Any ? Box(count > 0) : CountResult(node.ClrType, count);
                        }
                    default:
                        {
                            List<object?> values = new List<object?>(rows.Count);
                            foreach (TRow row in rows) values.Add(Bind(source, row).Visit(node.Operand!));
                            object? result = QueryAggregates.Compute(node.Function, values, QueryValueComparer.Ordinal);
                            return node.Function == AggregateFunction.Sum ? result ?? 0m : result;
                        }
                }
            }
            finally
            {
                Restore(source, hadPrevious, previous);
            }
        }

        /// <summary>
        /// Reads a column of a row, in the evaluator's comparable value form: the form values are compared, ordered and
        /// computed in. It must match what <see cref="NormalizeValue"/> returns for parameter values of the same column
        /// (for example both stored enum names, or both enum values). Null means the column is null.
        /// </summary>
        /// <param name="metadata">Entity the row belongs to. Never null.</param>
        /// <param name="row">Row. Never null.</param>
        /// <param name="column">Column of <paramref name="metadata"/>. Never null.</param>
        /// <returns>The value, or null.</returns>
        protected abstract object? GetValue(EntityMetadata metadata, TRow row, ColumnMetadata column);

        /// <summary>
        /// Returns the rows of an entity whose column equals a key, including soft-deleted rows (the evaluator excludes
        /// them). Used to resolve navigation members (the related row by its key), collections (related rows by foreign
        /// key) and many-to-many junction rows. Equality should be ordinal, as <see cref="QueryValueComparer.Ordinal"/>.
        /// </summary>
        /// <param name="metadata">Entity whose rows are searched. Never null.</param>
        /// <param name="column">Column of <paramref name="metadata"/> to match. Never null.</param>
        /// <param name="key">Key in the evaluator's value form. Never null.</param>
        /// <returns>The matching rows (possibly empty). Must not be null.</returns>
        protected abstract IEnumerable<TRow> FindRows(EntityMetadata metadata, ColumnMetadata column, object key);

        /// <summary>
        /// Converts a client-side value bound to a column (a <see cref="ValueNode"/> with a column, or an
        /// <see cref="InNode"/> list item) to the form <see cref="GetValue"/> returns for that column. The default returns
        /// the value unchanged; override it when the backend stores converted values (enum names or numbers,
        /// <see cref="IValueConverter"/> provider values, JSON text, ...).
        /// </summary>
        /// <param name="column">Column the value is compared with. Never null.</param>
        /// <param name="value">Model value; may be null.</param>
        /// <returns>The comparable value, or null.</returns>
        protected virtual object? NormalizeValue(ColumnMetadata column, object? value)
        {
            return value;
        }

        /// <summary>
        /// Returns the non-soft-deleted rows related to an owner key through a navigation: for many-to-many, through the
        /// junction entity; otherwise the rows of <paramref name="related"/> whose remote column equals the key. The
        /// default is built on <see cref="FindRows"/> and <see cref="IsSoftDeleted"/>; override it when the backend can
        /// resolve relationships more directly (for example graph edges).
        /// </summary>
        /// <param name="navigation">Navigation. Never null.</param>
        /// <param name="related">Related entity. Never null.</param>
        /// <param name="key">Owner key in the evaluator's value form. Never null.</param>
        /// <returns>A new list of the related rows. Never null.</returns>
        protected virtual List<TRow> RelatedRows(NavigationMetadata navigation, EntityMetadata related, object key)
        {
            List<TRow> rows = new List<TRow>();
            if (navigation.Kind == NavigationKind.ManyToMany)
            {
                EntityMetadata junction = EntityMetadata.For(navigation.JunctionType!);
                foreach (TRow link in FindRows(junction, navigation.JunctionLocalColumn!, key))
                {
                    object? remote = GetValue(junction, link, navigation.JunctionRemoteColumn!);
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

        private void Restore(QuerySource source, bool hadPrevious, TRow? previous)
        {
            if (hadPrevious) _Bindings[source] = previous!;
            else _Bindings.Remove(source);
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
