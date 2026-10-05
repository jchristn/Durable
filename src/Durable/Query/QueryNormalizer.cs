namespace Durable.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Durable;

    /// <summary>
    /// Converts LINQ expressions over mapped entities into the backend-neutral <see cref="QueryNode"/> tree.
    /// Client-side subtrees (constants, captured variables, method calls that do not touch a bound parameter) are
    /// evaluated once into <see cref="ValueNode"/>s; integers compared with enum columns become enum values; comparisons
    /// with null become <see cref="NullCheckNode"/>s; string methods carry a <see cref="StringMatchMode"/> taken from an
    /// explicit <see cref="StringComparison"/> argument or from <see cref="DefaultStringMatching"/>.
    /// Supported: comparisons, boolean logic, arithmetic, string concatenation, <c>??</c>, conditionals, Nullable
    /// HasValue/Value, string methods (Contains, StartsWith, EndsWith, Equals, CompareTo/Compare, ToUpper/ToLower, Trim,
    /// TrimStart, TrimEnd, Substring, Replace, IndexOf, Length, IsNullOrEmpty, IsNullOrWhiteSpace, Concat), collection
    /// Contains, <see cref="ExpressionExtensions"/> helpers, DateTime parts and Add* methods, Math functions, reference
    /// navigation members, collection navigation Any/All/Count, and grouping expressions (see <see cref="UseGrouping"/>).
    /// Thread safety: not thread-safe; create one per query translation.
    /// </summary>
    public class QueryNormalizer
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the string comparison mode used when a string comparison has no explicit
        /// <see cref="StringComparison"/> argument (for example <c>==</c> or <c>Contains(string)</c>).
        /// Default: <see cref="StringMatchMode.Database"/>.
        /// </summary>
        public StringMatchMode DefaultStringMatching { get; set; } = StringMatchMode.Database;

        /// <summary>
        /// Gets the active grouping, or null when the query is not grouped.
        /// </summary>
        public GroupingSpecification? Grouping { get; private set; }

        /// <summary>
        /// Gets the source the grouping's rows come from, or null when the query is not grouped.
        /// </summary>
        public QuerySource? GroupingSource { get; private set; }

        #endregion

        #region Private-Members

        private readonly Dictionary<ParameterExpression, QuerySource> _Sources = new Dictionary<ParameterExpression, QuerySource>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a normalizer.
        /// </summary>
        /// <param name="defaultStringMatching">Mode for string comparisons without an explicit <see cref="StringComparison"/>.</param>
        public QueryNormalizer(StringMatchMode defaultStringMatching = StringMatchMode.Database)
        {
            DefaultStringMatching = defaultStringMatching;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Binds a lambda parameter to a source.
        /// </summary>
        /// <param name="parameter">Lambda parameter. Must not be null.</param>
        /// <param name="source">Source. Must not be null.</param>
        /// <returns>This normalizer.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public QueryNormalizer Bind(ParameterExpression parameter, QuerySource source)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(source);
            _Sources[parameter] = source;
            return this;
        }

        /// <summary>
        /// Returns the source bound to a parameter, or null.
        /// </summary>
        /// <param name="parameter">Parameter. Must not be null.</param>
        /// <returns>The source or null.</returns>
        public QuerySource? SourceOf(ParameterExpression parameter)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            return _Sources.TryGetValue(parameter, out QuerySource? source) ? source : null;
        }

        /// <summary>
        /// Activates grouping translation: <c>g.Key</c> and its members become key-part nodes, and <c>g.Count()</c>,
        /// <c>g.Sum(...)</c>, <c>g.Average(...)</c>, <c>g.Min(...)</c>, <c>g.Max(...)</c>, <c>g.Any(...)</c> become
        /// <see cref="AggregateNode"/>s over rows of <paramref name="source"/>.
        /// </summary>
        /// <param name="grouping">Grouping. Must not be null.</param>
        /// <param name="source">Source of the grouped rows. Must not be null.</param>
        /// <returns>This normalizer.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public QueryNormalizer UseGrouping(GroupingSpecification grouping, QuerySource source)
        {
            Grouping = grouping ?? throw new ArgumentNullException(nameof(grouping));
            GroupingSource = source ?? throw new ArgumentNullException(nameof(source));
            Bind(grouping.KeySelector.Parameters[0], source);
            return this;
        }

        /// <summary>
        /// Normalizes the key parts of a grouping, binding its key selector to a source.
        /// </summary>
        /// <param name="grouping">Grouping. Must not be null.</param>
        /// <param name="source">Source of the grouped rows. Must not be null.</param>
        /// <returns>One node per key part, in order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public List<QueryNode> NormalizeGroupKey(GroupingSpecification grouping, QuerySource source)
        {
            ArgumentNullException.ThrowIfNull(grouping);
            ArgumentNullException.ThrowIfNull(source);
            Bind(grouping.KeySelector.Parameters[0], source);
            return grouping.KeyParts.Select(part => Normalize(part.Value)).ToList();
        }

        /// <summary>
        /// Normalizes a lambda body, binding its first parameter to a source.
        /// </summary>
        /// <param name="lambda">Lambda. Must not be null.</param>
        /// <param name="source">Source for the first parameter. Must not be null.</param>
        /// <returns>The node.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for constructs that cannot be translated.</exception>
        public QueryNode NormalizeLambda(LambdaExpression lambda, QuerySource source)
        {
            ArgumentNullException.ThrowIfNull(lambda);
            ArgumentNullException.ThrowIfNull(source);
            Bind(lambda.Parameters[0], source);
            return Normalize(lambda.Body);
        }

        /// <summary>
        /// Normalizes an expression whose parameters are already bound.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>The node.</returns>
        /// <exception cref="ArgumentNullException">Thrown when expression is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for constructs that cannot be translated.</exception>
        public QueryNode Normalize(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            expression = StripQuotes(expression);

            QueryNode? grouped = TryNormalizeGrouping(expression);
            if (grouped != null) return grouped;

            if (ExpressionEvaluator.IsEvaluable(expression))
                return new ValueNode(ExpressionEvaluator.Evaluate(expression), null, UnwrapConvert(expression).Type);

            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.And when expression.Type == typeof(bool):
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return new LogicalNode(LogicalOperator.And, Normalize(binary.Left), Normalize(binary.Right));
                    }
                case ExpressionType.OrElse:
                case ExpressionType.Or when expression.Type == typeof(bool):
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return new LogicalNode(LogicalOperator.Or, Normalize(binary.Left), Normalize(binary.Right));
                    }
                case ExpressionType.Not when expression.Type == typeof(bool) || expression.Type == typeof(bool?):
                    return new NotNode(Normalize(((UnaryExpression)expression).Operand));
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return NormalizeComparison(ToComparison(binary.NodeType), binary.Left, binary.Right, null);
                    }
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        if (binary.Type == typeof(string)) return new ConcatNode(FlattenConcat(binary).Select(NormalizeConcatPart).ToList());
                        return Arithmetic(binary, ArithmeticOperator.Add);
                    }
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return Arithmetic((BinaryExpression)expression, ArithmeticOperator.Subtract);
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return Arithmetic((BinaryExpression)expression, ArithmeticOperator.Multiply);
                case ExpressionType.Divide:
                    return Arithmetic((BinaryExpression)expression, ArithmeticOperator.Divide);
                case ExpressionType.Modulo:
                    return Arithmetic((BinaryExpression)expression, ArithmeticOperator.Modulo);
                case ExpressionType.Coalesce:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return new CoalesceNode(Normalize(binary.Left), Normalize(binary.Right), binary.Type);
                    }
                case ExpressionType.Conditional:
                    {
                        ConditionalExpression conditional = (ConditionalExpression)expression;
                        return new ConditionalNode(Normalize(conditional.Test), Normalize(conditional.IfTrue), Normalize(conditional.IfFalse), conditional.Type);
                    }
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.TypeAs:
                    return Normalize(((UnaryExpression)expression).Operand);
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    return new NegateNode(Normalize(((UnaryExpression)expression).Operand));
                case ExpressionType.MemberAccess:
                    return NormalizeMember((MemberExpression)expression);
                case ExpressionType.Call:
                    return NormalizeMethod((MethodCallExpression)expression);
                case ExpressionType.Constant:
                    return new ValueNode(((ConstantExpression)expression).Value, null, expression.Type);
                default:
                    throw Unsupported(expression);
            }
        }

        /// <summary>
        /// Resolves an expression to a mapped column when it is a direct member access on a bound parameter
        /// (ignoring conversions and <c>Nullable.Value</c>), without translating anything else.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>The column node, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when expression is null.</exception>
        public ColumnNode? ResolveColumn(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            expression = UnwrapConvert(StripQuotes(expression));
            if (expression is MemberExpression member && member.Member.Name == "Value" && member.Expression != null && Nullable.GetUnderlyingType(member.Expression.Type) != null)
                expression = UnwrapConvert(member.Expression);
            if (expression is MemberExpression m && m.Expression is ParameterExpression p && _Sources.TryGetValue(p, out QuerySource? source) && m.Member is PropertyInfo property)
            {
                ColumnMetadata? column = source.Metadata.FindColumn(property);
                if (column != null) return new ColumnNode(source, column);
            }

            return null;
        }

        /// <summary>
        /// Maps a <see cref="StringComparison"/> to the string match mode it requests: ordinal comparisons map to
        /// <see cref="StringMatchMode.Ordinal"/>, any ignore-case comparison to <see cref="StringMatchMode.IgnoreCase"/>,
        /// and culture-sensitive comparisons to <see cref="StringMatchMode.Database"/>.
        /// </summary>
        /// <param name="comparison">Comparison.</param>
        /// <returns>The mode.</returns>
        public static StringMatchMode ModeFor(StringComparison comparison)
        {
            switch (comparison)
            {
                case StringComparison.Ordinal:
                    return StringMatchMode.Ordinal;
                case StringComparison.OrdinalIgnoreCase:
                case StringComparison.CurrentCultureIgnoreCase:
                case StringComparison.InvariantCultureIgnoreCase:
                    return StringMatchMode.IgnoreCase;
                default:
                    return StringMatchMode.Database;
            }
        }

        #endregion

        #region Private-Methods

        private QueryNode Arithmetic(BinaryExpression binary, ArithmeticOperator op)
        {
            bool integer = op == ArithmeticOperator.Divide && IsIntegralType(binary.Left.Type) && IsIntegralType(binary.Right.Type);
            return new ArithmeticNode(op, Normalize(binary.Left), Normalize(binary.Right), integer, binary.Type);
        }

        private QueryNode NormalizeConcatPart(Expression expression)
        {
            expression = UnwrapConvert(expression);
            if (ExpressionEvaluator.IsEvaluable(expression))
            {
                object? value = ExpressionEvaluator.Evaluate(expression);
                string text = value == null ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
                return new ValueNode(text, null, typeof(string));
            }

            return Normalize(expression);
        }

        private static List<Expression> FlattenConcat(BinaryExpression binary)
        {
            List<Expression> parts = new List<Expression>();
            void Walk(Expression e)
            {
                if (e is BinaryExpression b && (b.NodeType == ExpressionType.Add || b.NodeType == ExpressionType.AddChecked) && b.Type == typeof(string))
                {
                    Walk(b.Left);
                    Walk(b.Right);
                }
                else if (e is MethodCallExpression call && call.Method.DeclaringType == typeof(string) && call.Method.Name == "Concat" && call.Arguments.All(a => a.Type == typeof(string) || a.Type == typeof(object)))
                {
                    foreach (Expression argument in call.Arguments) Walk(argument);
                }
                else
                {
                    parts.Add(e);
                }
            }

            Walk(binary);
            return parts;
        }

        private QueryNode NormalizeComparison(ComparisonOperator op, Expression left, Expression right, StringMatchMode? explicitMode)
        {
            QueryNode? compareTo = TryNormalizeCompareTo(left, right, op);
            if (compareTo != null) return compareTo;
            compareTo = TryNormalizeCompareTo(right, left, Flip(op));
            if (compareTo != null) return compareTo;

            StringMatchMode mode = IsStringType(left) || IsStringType(right) ? explicitMode ?? DefaultStringMatching : StringMatchMode.Database;
            bool leftEvaluable = ExpressionEvaluator.IsEvaluable(left);
            bool rightEvaluable = ExpressionEvaluator.IsEvaluable(right);

            if (leftEvaluable && rightEvaluable)
                return new ValueNode(EvaluateComparison(op, ExpressionEvaluator.Evaluate(left), ExpressionEvaluator.Evaluate(right)), null, typeof(bool));

            if (op == ComparisonOperator.Equal || op == ComparisonOperator.NotEqual)
            {
                Expression? other = null;
                if (leftEvaluable && ExpressionEvaluator.Evaluate(left) == null) other = right;
                else if (rightEvaluable && ExpressionEvaluator.Evaluate(right) == null) other = left;
                if (other != null) return new NullCheckNode(Normalize(other), op == ComparisonOperator.Equal);
            }

            if (leftEvaluable || rightEvaluable)
            {
                Expression columnSide = leftEvaluable ? right : left;
                Expression valueSide = leftEvaluable ? left : right;
                QueryNode columnNode = Normalize(columnSide);
                ValueNode valueNode = ValueFor(ExpressionEvaluator.Evaluate(valueSide), UnwrapConvert(valueSide).Type, (columnNode as ColumnNode)?.Column);
                return leftEvaluable
                    ? new ComparisonNode(op, valueNode, columnNode, mode)
                    : new ComparisonNode(op, columnNode, valueNode, mode);
            }

            return new ComparisonNode(op, Normalize(left), Normalize(right), mode);
        }

        private QueryNode? TryNormalizeCompareTo(Expression call, Expression zero, ComparisonOperator op)
        {
            if (UnwrapConvert(call) is not MethodCallExpression method) return null;
            if (!ExpressionEvaluator.IsEvaluable(zero) || !(ExpressionEvaluator.Evaluate(zero) is int z) || z != 0) return null;

            Expression? a = null;
            Expression? b = null;
            StringMatchMode? mode = null;
            if (method.Method.Name == "CompareTo" && method.Object != null && method.Arguments.Count == 1)
            {
                a = method.Object;
                b = method.Arguments[0];
            }
            else if (method.Method.DeclaringType == typeof(string) && method.Method.Name == "CompareOrdinal" && method.Arguments.Count == 2)
            {
                a = method.Arguments[0];
                b = method.Arguments[1];
                mode = StringMatchMode.Ordinal;
            }
            else if (method.Method.Name == "Compare" && method.Method.DeclaringType == typeof(string) && method.Arguments.Count >= 2)
            {
                a = method.Arguments[0];
                b = method.Arguments[1];
                if (method.Arguments.Count == 3) mode = ComparisonArgumentMode(method.Arguments[2]);
            }

            if (a == null || b == null) return null;
            return NormalizeComparison(op, a, b, mode);
        }

        private static StringMatchMode? ComparisonArgumentMode(Expression expression)
        {
            if (expression.Type == typeof(StringComparison) && ExpressionEvaluator.IsEvaluable(expression))
                return ModeFor((StringComparison)ExpressionEvaluator.Evaluate(expression)!);
            if (expression.Type == typeof(bool) && ExpressionEvaluator.IsEvaluable(expression))
                return (bool)ExpressionEvaluator.Evaluate(expression)! ? StringMatchMode.IgnoreCase : null;
            return null;
        }

        private static object EvaluateComparison(ComparisonOperator op, object? left, object? right)
        {
            if (op == ComparisonOperator.Equal) return Equals(left, right);
            if (op == ComparisonOperator.NotEqual) return !Equals(left, right);
            if (left == null || right == null) return false;
            int comparison = Comparer.Default.Compare(left, right);
            switch (op)
            {
                case ComparisonOperator.LessThan: return comparison < 0;
                case ComparisonOperator.LessThanOrEqual: return comparison <= 0;
                case ComparisonOperator.GreaterThan: return comparison > 0;
                default: return comparison >= 0;
            }
        }

        private static ComparisonOperator ToComparison(ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.Equal: return ComparisonOperator.Equal;
                case ExpressionType.NotEqual: return ComparisonOperator.NotEqual;
                case ExpressionType.LessThan: return ComparisonOperator.LessThan;
                case ExpressionType.LessThanOrEqual: return ComparisonOperator.LessThanOrEqual;
                case ExpressionType.GreaterThan: return ComparisonOperator.GreaterThan;
                case ExpressionType.GreaterThanOrEqual: return ComparisonOperator.GreaterThanOrEqual;
                default: throw new NotSupportedException("Operator " + type + " is not a comparison.");
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

        private static ValueNode ValueFor(object? value, Type type, ColumnMetadata? column)
        {
            if (value != null && column != null && column.IsEnum && column.Converter == null && !value.GetType().IsEnum && IsIntegral(value))
                value = Enum.ToObject(column.ClrType, value);
            return new ValueNode(value, column, type);
        }

        private QueryNode NormalizeMember(MemberExpression member)
        {
            if (member.Expression == null)
                throw Unsupported(member);

            if (member.Expression is ParameterExpression parameter)
            {
                if (!_Sources.TryGetValue(parameter, out QuerySource? source))
                    throw new NotSupportedException("Parameter '" + parameter.Name + "' is not bound to a source in this query.");
                if (member.Member is PropertyInfo property)
                {
                    ColumnMetadata? column = source.Metadata.FindColumn(property);
                    if (column != null) return new ColumnNode(source, column);
                    if (source.Metadata.FindNavigation(property.Name) != null)
                        throw new NotSupportedException("Navigation '" + property.Name + "' cannot be used as a value; compare one of its members instead.");
                }

                throw new NotSupportedException("Member '" + member.Member.Name + "' of " + source.Metadata.EntityType.Name + " is not a mapped column.");
            }

            Type declaring = member.Expression.Type;
            Type? nullableUnderlying = Nullable.GetUnderlyingType(declaring);
            if (nullableUnderlying != null)
            {
                if (member.Member.Name == "HasValue") return new NullCheckNode(Normalize(member.Expression), false);
                if (member.Member.Name == "Value") return Normalize(member.Expression);
            }

            if (declaring == typeof(string) && member.Member.Name == "Length")
                return new FunctionNode(QueryFunction.Length, new[] { Normalize(member.Expression) }, StringMatchMode.Database, typeof(int));

            Type dateType = nullableUnderlying ?? declaring;
            if (dateType == typeof(DateTime) || dateType == typeof(DateTimeOffset) || dateType == typeof(DateOnly))
            {
                QueryFunction? part = member.Member.Name switch
                {
                    "Year" => QueryFunction.Year,
                    "Month" => QueryFunction.Month,
                    "Day" => QueryFunction.Day,
                    "Hour" => QueryFunction.Hour,
                    "Minute" => QueryFunction.Minute,
                    "Second" => QueryFunction.Second,
                    "DayOfYear" => QueryFunction.DayOfYear,
                    "DayOfWeek" => QueryFunction.DayOfWeek,
                    "Date" => QueryFunction.Date,
                    _ => null
                };
                if (part.HasValue) return new FunctionNode(part.Value, new[] { Normalize(member.Expression) }, StringMatchMode.Database, member.Type);
            }

            if (member.Member.Name == "Count" && TryGetCollectionNavigation(member.Expression, out NavigationMetadata? countNavigation, out Expression? countOwner))
                return Collection(CollectionOperation.Count, countNavigation!, countOwner!, null, member.Type);

            if (TryGetReferenceNavigation(member.Expression, out NavigationMetadata? navigation, out Expression? owner) && member.Member is PropertyInfo relatedProperty)
            {
                EntityMetadata related = EntityMetadata.For(navigation!.RelatedType);
                ColumnMetadata? relatedColumn = related.FindColumn(relatedProperty);
                if (relatedColumn == null)
                    throw new NotSupportedException("Member '" + relatedProperty.Name + "' of " + related.EntityType.Name + " is not a mapped column.");

                QueryNode ownerKey = Normalize(Expression.Property(owner!, navigation.LocalColumn.Property));
                return new NavigationMemberNode(navigation, ownerKey, new QuerySource(related, navigation.Name), relatedColumn, member.Type);
            }

            throw Unsupported(member);
        }

        private CollectionNode Collection(CollectionOperation operation, NavigationMetadata navigation, Expression owner, LambdaExpression? predicate, Type type)
        {
            QueryNode ownerKey = Normalize(Expression.Property(owner, navigation.LocalColumn.Property));
            QuerySource related = new QuerySource(EntityMetadata.For(navigation.RelatedType), navigation.Name);
            QueryNode? condition = predicate == null ? null : NormalizeLambda(predicate, related);
            return new CollectionNode(operation, navigation, ownerKey, related, condition, type);
        }

        private bool TryGetReferenceNavigation(Expression expression, out NavigationMetadata? navigation, out Expression? owner)
        {
            navigation = null;
            owner = null;
            if (expression is not MemberExpression navigationAccess || navigationAccess.Expression == null) return false;
            if (!IsEntityExpression(navigationAccess.Expression)) return false;
            NavigationMetadata? candidate = EntityMetadata.For(navigationAccess.Expression.Type).FindNavigation(navigationAccess.Member.Name);
            if (candidate == null || candidate.Kind != NavigationKind.Reference) return false;
            navigation = candidate;
            owner = navigationAccess.Expression;
            return true;
        }

        private bool TryGetCollectionNavigation(Expression expression, out NavigationMetadata? navigation, out Expression? owner)
        {
            navigation = null;
            owner = null;
            expression = UnwrapConvert(expression);
            if (expression is not MemberExpression navigationAccess || navigationAccess.Expression == null) return false;
            if (!IsEntityExpression(navigationAccess.Expression)) return false;
            NavigationMetadata? candidate = EntityMetadata.For(navigationAccess.Expression.Type).FindNavigation(navigationAccess.Member.Name);
            if (candidate == null || candidate.Kind == NavigationKind.Reference) return false;
            navigation = candidate;
            owner = navigationAccess.Expression;
            return true;
        }

        private bool IsEntityExpression(Expression expression)
        {
            if (expression is ParameterExpression parameter) return _Sources.ContainsKey(parameter);
            return TryGetReferenceNavigation(expression, out _, out _);
        }

        private QueryNode NormalizeMethod(MethodCallExpression call)
        {
            MethodInfo method = call.Method;
            Type? declaring = method.DeclaringType;
            string name = method.Name;

            if (declaring == typeof(string))
            {
                QueryNode? stringNode = TryNormalizeStringMethod(call);
                if (stringNode != null) return stringNode;
            }

            if (name == "Contains")
            {
                QueryNode? contains = TryNormalizeCollectionContains(call);
                if (contains != null) return contains;
            }

            if (declaring == typeof(Enumerable) && call.Arguments.Count >= 1 && TryGetCollectionNavigation(call.Arguments[0], out NavigationMetadata? navigation, out Expression? owner))
            {
                LambdaExpression? lambda = call.Arguments.Count > 1 ? (LambdaExpression)StripQuotes(call.Arguments[1]) : null;
                switch (name)
                {
                    case "Any":
                        return Collection(CollectionOperation.Any, navigation!, owner!, lambda, typeof(bool));
                    case "All":
                        if (lambda == null) throw Unsupported(call);
                        return Collection(CollectionOperation.All, navigation!, owner!, lambda, typeof(bool));
                    case "Count":
                    case "LongCount":
                        return Collection(CollectionOperation.Count, navigation!, owner!, lambda, call.Type);
                }
            }

            if (declaring == typeof(ExpressionExtensions))
            {
                switch (name)
                {
                    case "Between":
                        {
                            QueryNode value = Normalize(call.Arguments[0]);
                            ColumnMetadata? column = (value as ColumnNode)?.Column;
                            StringMatchMode mode = IsStringType(call.Arguments[0]) ? DefaultStringMatching : StringMatchMode.Database;
                            return new LogicalNode(
                                LogicalOperator.And,
                                new ComparisonNode(ComparisonOperator.GreaterThanOrEqual, value, Operand(call.Arguments[1], column), mode),
                                new ComparisonNode(ComparisonOperator.LessThanOrEqual, value, Operand(call.Arguments[2], column), mode));
                        }
                    case "In":
                    case "NotIn":
                        return In(call.Arguments[0], call.Arguments[1], name == "NotIn");
                    case "IsNull":
                        return new NullCheckNode(Normalize(call.Arguments[0]), true);
                    case "IsNotNull":
                        return new NullCheckNode(Normalize(call.Arguments[0]), false);
                }
            }

            if (declaring == typeof(Math) || declaring == typeof(decimal) && (name == "Round" || name == "Floor" || name == "Ceiling" || name == "Abs"))
            {
                List<QueryNode> arguments = call.Arguments.Where(a => a.Type != typeof(MidpointRounding)).Select(Normalize).ToList();
                QueryFunction? function = name switch
                {
                    "Abs" => QueryFunction.Abs,
                    "Round" => QueryFunction.Round,
                    "Ceiling" => QueryFunction.Ceiling,
                    "Floor" => QueryFunction.Floor,
                    "Pow" => QueryFunction.Power,
                    "Sqrt" => QueryFunction.Sqrt,
                    _ => null
                };
                if (function.HasValue) return new FunctionNode(function.Value, arguments, StringMatchMode.Database, call.Type);
            }

            if (call.Object != null && (call.Object.Type == typeof(DateTime) || call.Object.Type == typeof(DateTimeOffset) || call.Object.Type == typeof(DateOnly)) && name.StartsWith("Add", StringComparison.Ordinal))
            {
                QueryFunction? function = name switch
                {
                    "AddYears" => QueryFunction.AddYears,
                    "AddMonths" => QueryFunction.AddMonths,
                    "AddDays" => QueryFunction.AddDays,
                    "AddHours" => QueryFunction.AddHours,
                    "AddMinutes" => QueryFunction.AddMinutes,
                    "AddSeconds" => QueryFunction.AddSeconds,
                    _ => null
                };
                if (function.HasValue)
                    return new FunctionNode(function.Value, new[] { Normalize(call.Object), Normalize(call.Arguments[0]) }, StringMatchMode.Database, call.Type);
            }

            if (name == "Equals")
            {
                if (call.Object != null && call.Arguments.Count == 1)
                    return NormalizeComparison(ComparisonOperator.Equal, UnwrapConvert(call.Object), call.Arguments[0], null);
                if (call.Object == null && call.Arguments.Count == 2)
                    return NormalizeComparison(ComparisonOperator.Equal, UnwrapConvert(call.Arguments[0]), call.Arguments[1], null);
            }

            throw Unsupported(call);
        }

        private QueryNode Operand(Expression expression, ColumnMetadata? column)
        {
            if (ExpressionEvaluator.IsEvaluable(expression))
                return ValueFor(ExpressionEvaluator.Evaluate(expression), UnwrapConvert(expression).Type, column);
            return Normalize(expression);
        }

        private QueryNode? TryNormalizeStringMethod(MethodCallExpression call)
        {
            string name = call.Method.Name;
            Expression? instance = call.Object;

            if (instance == null)
            {
                switch (name)
                {
                    case "IsNullOrEmpty":
                        return new StringTestNode(StringTestKind.IsNullOrEmpty, Normalize(call.Arguments[0]));
                    case "IsNullOrWhiteSpace":
                        return new StringTestNode(StringTestKind.IsNullOrWhiteSpace, Normalize(call.Arguments[0]));
                    case "Concat":
                        if (call.Arguments.Count == 1 && call.Arguments[0] is NewArrayExpression array)
                            return new ConcatNode(array.Expressions.Select(NormalizeConcatPart).ToList());
                        return new ConcatNode(call.Arguments.Select(NormalizeConcatPart).ToList());
                    case "Equals":
                        if (call.Arguments.Count == 3 && call.Arguments[2].Type == typeof(StringComparison))
                            return NormalizeComparison(ComparisonOperator.Equal, call.Arguments[0], call.Arguments[1], ComparisonArgumentMode(call.Arguments[2]));
                        return null;
                }

                return null;
            }

            switch (name)
            {
                case "Contains":
                case "StartsWith":
                case "EndsWith":
                    {
                        if (call.Arguments.Count == 0) return null;
                        Expression argument = call.Arguments[0];
                        Expression last = call.Arguments[call.Arguments.Count - 1];
                        StringMatchMode mode = call.Arguments.Count > 1 && last.Type == typeof(StringComparison)
                            ? ComparisonArgumentMode(last) ?? DefaultStringMatching
                            : DefaultStringMatching;
                        if (call.Arguments.Count == 3 && call.Arguments[1].Type == typeof(bool))
                            mode = ComparisonArgumentMode(call.Arguments[1]) ?? DefaultStringMatching;

                        QueryNode pattern;
                        if (ExpressionEvaluator.IsEvaluable(argument))
                        {
                            object? raw = ExpressionEvaluator.Evaluate(argument);
                            if (raw == null) return new ValueNode(false, null, typeof(bool));
                            pattern = new ValueNode(Convert.ToString(raw, CultureInfo.InvariantCulture)!, null, typeof(string));
                        }
                        else
                        {
                            pattern = Normalize(argument);
                        }

                        StringMatchKind kind = name == "Contains" ? StringMatchKind.Contains : name == "StartsWith" ? StringMatchKind.StartsWith : StringMatchKind.EndsWith;
                        return new StringMatchNode(kind, Normalize(instance), pattern, mode);
                    }
                case "Equals":
                    if (call.Arguments.Count == 2 && call.Arguments[1].Type == typeof(StringComparison))
                        return NormalizeComparison(ComparisonOperator.Equal, instance, call.Arguments[0], ComparisonArgumentMode(call.Arguments[1]));
                    return null;
                case "CompareTo":
                    return null;
                case "ToUpper":
                case "ToUpperInvariant":
                    return new FunctionNode(QueryFunction.Upper, new[] { Normalize(instance) }, StringMatchMode.Database, typeof(string));
                case "ToLower":
                case "ToLowerInvariant":
                    return new FunctionNode(QueryFunction.Lower, new[] { Normalize(instance) }, StringMatchMode.Database, typeof(string));
                case "Trim":
                    if (call.Arguments.Count > 0) return null;
                    return new FunctionNode(QueryFunction.Trim, new[] { Normalize(instance) }, StringMatchMode.Database, typeof(string));
                case "TrimStart":
                    if (call.Arguments.Count > 0 && !IsEmptyCharArray(call.Arguments[0])) return null;
                    return new FunctionNode(QueryFunction.TrimStart, new[] { Normalize(instance) }, StringMatchMode.Database, typeof(string));
                case "TrimEnd":
                    if (call.Arguments.Count > 0 && !IsEmptyCharArray(call.Arguments[0])) return null;
                    return new FunctionNode(QueryFunction.TrimEnd, new[] { Normalize(instance) }, StringMatchMode.Database, typeof(string));
                case "Substring":
                    {
                        List<QueryNode> arguments = new List<QueryNode> { Normalize(instance) };
                        arguments.AddRange(call.Arguments.Select(Normalize));
                        return new FunctionNode(QueryFunction.Substring, arguments, StringMatchMode.Database, typeof(string));
                    }
                case "Replace":
                    if (call.Arguments.Count != 2 || call.Arguments[0].Type != typeof(string)) return null;
                    return new FunctionNode(QueryFunction.Replace, new[] { Normalize(instance), Normalize(call.Arguments[0]), Normalize(call.Arguments[1]) }, DefaultStringMatching, typeof(string));
                case "IndexOf":
                    {
                        if (call.Arguments.Count == 0 || call.Arguments[0].Type != typeof(string)) return null;
                        StringMatchMode mode = DefaultStringMatching;
                        if (call.Arguments.Count == 2 && call.Arguments[1].Type == typeof(StringComparison)) mode = ComparisonArgumentMode(call.Arguments[1]) ?? DefaultStringMatching;
                        else if (call.Arguments.Count != 1) return null;
                        return new FunctionNode(QueryFunction.IndexOf, new[] { Normalize(instance), Normalize(call.Arguments[0]) }, mode, typeof(int));
                    }
            }

            return null;
        }

        private static bool IsEmptyCharArray(Expression expression)
        {
            if (!ExpressionEvaluator.IsEvaluable(expression)) return false;
            object? value = ExpressionEvaluator.Evaluate(expression);
            return value == null || (value is char[] chars && chars.Length == 0);
        }

        private QueryNode? TryNormalizeCollectionContains(MethodCallExpression call)
        {
            Expression? collection;
            Expression? item;
            if (call.Object == null && call.Method.DeclaringType == typeof(MemoryExtensions)
                && (call.Arguments.Count == 2 || (call.Arguments.Count == 3 && IsNullConstant(call.Arguments[2]))))
            {
                collection = UnwrapSpanConversion(call.Arguments[0]);
                item = call.Arguments[1];
            }
            else if (call.Object == null && call.Arguments.Count == 2)
            {
                collection = call.Arguments[0];
                item = call.Arguments[1];
            }
            else if (call.Object != null && call.Arguments.Count == 1 && call.Object.Type != typeof(string))
            {
                collection = call.Object;
                item = call.Arguments[0];
            }
            else
            {
                return null;
            }

            if (collection.Type == typeof(string)) return null;
            if (!ExpressionEvaluator.IsEvaluable(collection))
            {
                if (TryGetCollectionNavigation(collection, out _, out _))
                    throw new NotSupportedException("Contains on a navigation collection is not supported; use Any(x => ...) instead.");
                return null;
            }

            return In(item, collection, false);
        }

        private QueryNode In(Expression item, Expression collection, bool negated)
        {
            if (!ExpressionEvaluator.IsEvaluable(collection))
                throw new NotSupportedException("The collection in an IN expression must be a client-side value.");

            object? source = ExpressionEvaluator.Evaluate(collection);
            QueryNode itemNode = Normalize(item);
            ColumnMetadata? column = (itemNode as ColumnNode)?.Column;
            Type itemType = UnwrapConvert(item).Type;
            List<object?> values = new List<object?>();
            if (source is IEnumerable enumerable && source is not string)
            {
                foreach (object? value in enumerable) values.Add(ValueFor(value, itemType, column).Value);
            }
            else if (source != null)
            {
                values.Add(ValueFor(source, itemType, column).Value);
            }

            StringMatchMode mode = itemType == typeof(string) ? DefaultStringMatching : StringMatchMode.Database;
            return new InNode(itemNode, values, negated, mode);
        }

        private QueryNode? TryNormalizeGrouping(Expression expression)
        {
            GroupingSpecification? grouping = Grouping;
            if (grouping == null) return null;

            if (expression is MemberExpression member)
            {
                if (member.Member.Name == "Key" && grouping.IsGrouping(member.Expression))
                {
                    if (grouping.KeyParts.Count != 1)
                        throw new NotSupportedException("A composite group key cannot be used as a single value; reference its members (g.Key.Name).");
                    return Normalize(grouping.KeyParts[0].Value);
                }

                if (member.Expression is MemberExpression keyAccess && keyAccess.Member.Name == "Key" && grouping.IsGrouping(keyAccess.Expression))
                {
                    foreach (KeyValuePair<string?, Expression> part in grouping.KeyParts)
                    {
                        if (part.Key == member.Member.Name) return Normalize(part.Value);
                    }

                    if (grouping.KeyParts.Count == 1) return null;
                    throw new NotSupportedException("Group key has no member '" + member.Member.Name + "'.");
                }

                return null;
            }

            if (expression is MethodCallExpression call && call.Method.DeclaringType == typeof(Enumerable) && call.Arguments.Count >= 1 && grouping.IsGrouping(call.Arguments[0]))
            {
                LambdaExpression? lambda = call.Arguments.Count > 1 ? StripQuotes(call.Arguments[1]) as LambdaExpression : null;
                QuerySource source = GroupingSource!;
                switch (call.Method.Name)
                {
                    case "Count":
                    case "LongCount":
                        return new AggregateNode(AggregateFunction.Count, lambda == null ? null : NormalizeLambda(lambda, source), call.Type);
                    case "Any":
                        return new AggregateNode(AggregateFunction.Any, lambda == null ? null : NormalizeLambda(lambda, source), typeof(bool));
                    case "Sum":
                    case "Min":
                    case "Max":
                    case "Average":
                        {
                            if (lambda == null) throw new NotSupportedException(call.Method.Name + " over a group requires a selector.");
                            AggregateFunction function = call.Method.Name switch
                            {
                                "Sum" => AggregateFunction.Sum,
                                "Min" => AggregateFunction.Min,
                                "Max" => AggregateFunction.Max,
                                _ => AggregateFunction.Average
                            };
                            return new AggregateNode(function, NormalizeLambda(lambda, source), call.Type);
                        }
                }

                throw new NotSupportedException("Group method '" + call.Method.Name + "' is not supported in queries.");
            }

            return null;
        }

        private static Expression UnwrapSpanConversion(Expression expression)
        {
            if (expression is MethodCallExpression call && call.Method.Name == "op_Implicit" && call.Arguments.Count == 1 && IsSpanType(call.Type))
                return call.Arguments[0];
            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert && IsSpanType(unary.Type))
                return unary.Operand;
            return expression;
        }

        private static bool IsSpanType(Type type)
        {
            if (!type.IsGenericType) return false;
            Type definition = type.GetGenericTypeDefinition();
            return definition == typeof(ReadOnlySpan<>) || definition == typeof(Span<>);
        }

        private static bool IsNullConstant(Expression expression)
        {
            return expression is ConstantExpression constant && constant.Value == null;
        }

        private static bool IsStringType(Expression expression)
        {
            return UnwrapConvert(expression).Type == typeof(string);
        }

        private static bool IsIntegralType(Type type)
        {
            Type t = Nullable.GetUnderlyingType(type) ?? type;
            return t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte)
                || t == typeof(uint) || t == typeof(ulong) || t == typeof(ushort) || t == typeof(sbyte);
        }

        private static bool IsIntegral(object? value)
        {
            return value is int || value is long || value is short || value is byte || value is uint || value is ulong || value is ushort || value is sbyte;
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote) expression = ((UnaryExpression)expression).Operand;
            return expression;
        }

        private static Expression UnwrapConvert(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked || expression.NodeType == ExpressionType.TypeAs)
                expression = ((UnaryExpression)expression).Operand;
            return expression;
        }

        private static NotSupportedException Unsupported(Expression expression)
        {
            return new NotSupportedException("The expression '" + expression + "' (" + expression.NodeType + ") cannot be translated to a query. Evaluate it on the client or use a raw predicate.");
        }

        #endregion
    }
}
