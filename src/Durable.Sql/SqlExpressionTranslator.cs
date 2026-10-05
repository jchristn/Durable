namespace Durable.Sql
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
    /// Translates LINQ expressions into SQL fragments for any <see cref="ISqlDialect"/>. Every client-side value is bound
    /// as a parameter (converted with the target column's rules when compared to a column), so the generated SQL is
    /// injection-safe, culture-invariant and plan-cache friendly.
    /// Supported: comparisons (with C# null semantics for nullable columns), boolean logic, arithmetic, string concatenation,
    /// coalesce, conditionals, enums (string or integer storage), Nullable HasValue/Value, string methods (Contains,
    /// StartsWith, EndsWith, Equals with case-insensitive comparison, ToUpper/ToLower, Trim, Substring, Replace, IndexOf,
    /// Length, IsNullOrEmpty, IsNullOrWhiteSpace), collection Contains (IN), DateTime parts and Add* methods, Math functions,
    /// <see cref="ExpressionExtensions"/> helpers, reference navigation members (correlated subqueries) and collection
    /// navigation Any/All/Count (EXISTS / COUNT subqueries).
    /// Thread safety: not thread-safe; create one per statement.
    /// </summary>
    public class SqlExpressionTranslator
    {
        #region Public-Members

        /// <summary>
        /// Gets the statement builder receiving parameters. Never null.
        /// </summary>
        public SqlStatementBuilder Builder { get; }

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect => Builder.Dialect;

        /// <summary>
        /// Gets the converter used for parameter values. Never null.
        /// </summary>
        public IDataTypeConverter Converter { get; }

        /// <summary>
        /// Gets or sets a hook tried before built-in translation; return null to fall through. Used for grouping translation.
        /// </summary>
        public Func<Expression, SqlExpressionTranslator, string?>? CustomTranslator { get; set; }

        #endregion

        #region Private-Members

        private const int InlineIntegerListThreshold = 500;
        private readonly Dictionary<ParameterExpression, TableSource> _Sources = new Dictionary<ParameterExpression, TableSource>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a translator.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="converter">Converter; null uses the dialect's.</param>
        /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
        public SqlExpressionTranslator(SqlStatementBuilder builder, IDataTypeConverter? converter = null)
        {
            Builder = builder ?? throw new ArgumentNullException(nameof(builder));
            Converter = converter ?? builder.Dialect.Converter;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Binds a lambda parameter to a table.
        /// </summary>
        /// <param name="parameter">Lambda parameter. Must not be null.</param>
        /// <param name="source">Table source. Must not be null.</param>
        /// <returns>This translator.</returns>
        public SqlExpressionTranslator Bind(ParameterExpression parameter, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(source);
            _Sources[parameter] = source;
            return this;
        }

        /// <summary>
        /// Translates a lambda body as a boolean condition, binding its single parameter to the source.
        /// </summary>
        /// <param name="lambda">Predicate lambda. Must not be null.</param>
        /// <param name="source">Source for the lambda parameter. Must not be null.</param>
        /// <returns>SQL condition.</returns>
        public string TranslatePredicate(LambdaExpression lambda, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(lambda);
            Bind(lambda.Parameters[0], source);
            return Predicate(lambda.Body);
        }

        /// <summary>
        /// Translates an expression as a boolean condition.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>SQL condition.</returns>
        /// <exception cref="NotSupportedException">Thrown for untranslatable constructs.</exception>
        public string Predicate(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            expression = StripQuotes(expression);

            string? custom = CustomTranslator?.Invoke(expression, this);
            if (custom != null)
                return IsPredicateNode(expression) ? custom : "(" + custom + " = " + Dialect.BooleanLiteral(true) + ")";

            if (ExpressionEvaluator.IsEvaluable(expression) && expression.Type == typeof(bool))
            {
                object? constant = ExpressionEvaluator.Evaluate(expression);
                return constant is bool b && b ? "(1 = 1)" : "(1 = 0)";
            }

            if (IsPredicateNode(expression)) return Translate(expression);
            return "(" + Value(expression) + " = " + Dialect.BooleanLiteral(true) + ")";
        }

        /// <summary>
        /// Translates an expression as a value (column, parameter, function or computed expression).
        /// Boolean conditions in value position become CASE expressions.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>SQL value expression.</returns>
        /// <exception cref="NotSupportedException">Thrown for untranslatable constructs.</exception>
        public string Value(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            expression = StripQuotes(expression);

            string? custom = CustomTranslator?.Invoke(expression, this);
            if (custom != null) return custom;

            if (ExpressionEvaluator.IsEvaluable(expression))
                return Builder.AddParameter(Converter.ConvertToDatabase(ExpressionEvaluator.Evaluate(expression), null));

            if (IsPredicateNode(expression))
                return "CASE WHEN " + Translate(expression) + " THEN " + Dialect.BooleanLiteral(true) + " ELSE " + Dialect.BooleanLiteral(false) + " END";

            return Translate(expression);
        }

        /// <summary>
        /// Resolves an expression to a mapped column when it is a direct member access on a bound parameter
        /// (ignoring conversions and Nullable.Value).
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>The column and its source, or null.</returns>
        public ColumnReference? ResolveColumn(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            expression = UnwrapConvert(StripQuotes(expression));
            if (expression is MemberExpression member && member.Member.Name == "Value" && member.Expression != null && Nullable.GetUnderlyingType(member.Expression.Type) != null)
                expression = UnwrapConvert(member.Expression);
            if (expression is MemberExpression m && m.Expression is ParameterExpression p && _Sources.TryGetValue(p, out TableSource? source) && m.Member is PropertyInfo property)
            {
                ColumnMetadata? column = source.Metadata.FindColumn(property);
                if (column != null) return new ColumnReference(source, column);
            }

            return null;
        }

        /// <summary>
        /// Returns the qualified SQL for a column.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>Qualified, quoted column reference.</returns>
        public string ColumnSql(TableSource source, ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(column);
            string quoted = Dialect.QuoteIdentifier(column.Name);
            return source.Qualifier == null ? quoted : source.Qualifier + "." + quoted;
        }

        /// <summary>
        /// Binds a client value as a parameter, converting it with the column's rules when a column is given.
        /// Integral values destined for enum columns are converted to the enum first.
        /// </summary>
        /// <param name="value">Value; may be null.</param>
        /// <param name="column">Target column; may be null.</param>
        /// <returns>The parameter placeholder.</returns>
        public string Parameter(object? value, ColumnMetadata? column)
        {
            return Builder.AddParameter(ConvertForColumn(value, column), column);
        }

        /// <summary>
        /// Returns the soft-delete condition for an entity (rows not deleted), or null when it has no soft-delete column.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <returns>SQL condition or null.</returns>
        public string? SoftDeleteFilter(TableSource source)
        {
            ArgumentNullException.ThrowIfNull(source);
            ColumnMetadata? column = source.Metadata.SoftDeleteColumn;
            if (column == null) return null;
            string sql = ColumnSql(source, column);
            if (column.ClrType == typeof(bool)) return "(" + sql + " = " + Dialect.BooleanLiteral(false) + " OR " + sql + " IS NULL)";
            return sql + " IS NULL";
        }

        #endregion

        #region Private-Methods

        private string Translate(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.And when expression.Type == typeof(bool):
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return "(" + Predicate(binary.Left) + " AND " + Predicate(binary.Right) + ")";
                    }
                case ExpressionType.OrElse:
                case ExpressionType.Or when expression.Type == typeof(bool):
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return "(" + Predicate(binary.Left) + " OR " + Predicate(binary.Right) + ")";
                    }
                case ExpressionType.Not when expression.Type == typeof(bool) || expression.Type == typeof(bool?):
                    {
                        Expression operand = ((UnaryExpression)expression).Operand;
                        string inner = Predicate(operand);
                        // SQL three-valued logic would drop rows where the operand is UNKNOWN (a NULL compared);
                        // C# evaluates such comparisons to false, so the negation is true.
                        if (MayBeUnknown(operand))
                            return "(CASE WHEN " + inner + " THEN " + Dialect.BooleanLiteral(false) + " ELSE " + Dialect.BooleanLiteral(true) + " END = " + Dialect.BooleanLiteral(true) + ")";
                        return "(NOT " + inner + ")";
                    }
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    return TranslateComparison((BinaryExpression)expression);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        if (binary.Type == typeof(string))
                            return Dialect.Concat(FlattenConcat(binary).Select(StringOperand).ToList());
                        return "(" + Value(binary.Left) + " + " + Value(binary.Right) + ")";
                    }
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return Arithmetic((BinaryExpression)expression, "-");
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return Arithmetic((BinaryExpression)expression, "*");
                case ExpressionType.Divide:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return Dialect.Divide(Value(binary.Left), Value(binary.Right), IsIntegralType(binary.Left.Type) && IsIntegralType(binary.Right.Type));
                    }
                case ExpressionType.Modulo:
                    return Arithmetic((BinaryExpression)expression, "%");
                case ExpressionType.Coalesce:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return "COALESCE(" + Value(binary.Left) + ", " + Value(binary.Right) + ")";
                    }
                case ExpressionType.Conditional:
                    {
                        ConditionalExpression conditional = (ConditionalExpression)expression;
                        return "CASE WHEN " + Predicate(conditional.Test) + " THEN " + Value(conditional.IfTrue) + " ELSE " + Value(conditional.IfFalse) + " END";
                    }
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.TypeAs:
                    return Value(((UnaryExpression)expression).Operand);
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    return "(-" + Value(((UnaryExpression)expression).Operand) + ")";
                case ExpressionType.MemberAccess:
                    return TranslateMember((MemberExpression)expression);
                case ExpressionType.Call:
                    return TranslateMethod((MethodCallExpression)expression);
                case ExpressionType.Constant:
                    return Builder.AddParameter(Converter.ConvertToDatabase(((ConstantExpression)expression).Value, null));
                default:
                    throw Unsupported(expression);
            }
        }

        private string Arithmetic(BinaryExpression binary, string op)
        {
            return "(" + Value(binary.Left) + " " + op + " " + Value(binary.Right) + ")";
        }

        private string StringOperand(Expression expression)
        {
            expression = UnwrapConvert(expression);
            if (expression.Type != typeof(string) && ExpressionEvaluator.IsEvaluable(expression))
            {
                object? value = ExpressionEvaluator.Evaluate(expression);
                return Builder.AddParameter(value == null ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture));
            }

            if (ExpressionEvaluator.IsEvaluable(expression))
            {
                object? value = ExpressionEvaluator.Evaluate(expression);
                return Builder.AddParameter(value ?? string.Empty);
            }

            string sql = Value(expression);
            if (expression.Type != typeof(string)) return "CAST(" + sql + " AS " + StringCastType() + ")";
            return "COALESCE(" + sql + ", '')";
        }

        private string StringCastType()
        {
            if (Dialect.RepositoryType == RepositoryType.MySql) return "CHAR";
            if (Dialect.RepositoryType == RepositoryType.SqlServer) return "NVARCHAR(MAX)";
            return "TEXT";
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

        private string TranslateComparison(BinaryExpression binary)
        {
            string op = ComparisonOperator(binary.NodeType);
            Expression left = binary.Left;
            Expression right = binary.Right;

            if (TryTranslateCompareTo(left, right, binary.NodeType, out string? compareSql)) return compareSql!;
            if (TryTranslateCompareTo(right, left, Flip(binary.NodeType), out compareSql)) return compareSql!;

            bool leftEvaluable = ExpressionEvaluator.IsEvaluable(left);
            bool rightEvaluable = ExpressionEvaluator.IsEvaluable(right);

            if (leftEvaluable && rightEvaluable)
            {
                object? constant = ExpressionEvaluator.Evaluate(binary);
                return constant is bool b && b ? "(1 = 1)" : "(1 = 0)";
            }

            if (binary.NodeType == ExpressionType.Equal || binary.NodeType == ExpressionType.NotEqual)
            {
                Expression? other = null;
                if (leftEvaluable && ExpressionEvaluator.Evaluate(left) == null) other = right;
                else if (rightEvaluable && ExpressionEvaluator.Evaluate(right) == null) other = left;
                if (other != null)
                    return "(" + Value(other) + (binary.NodeType == ExpressionType.Equal ? " IS NULL)" : " IS NOT NULL)");
            }

            if (leftEvaluable || rightEvaluable)
            {
                Expression columnSide = leftEvaluable ? right : left;
                Expression valueSide = leftEvaluable ? left : right;
                ColumnReference? reference = ResolveColumn(columnSide);
                string columnSql = Value(columnSide);
                object? value = ExpressionEvaluator.Evaluate(valueSide);
                string parameter = Parameter(value, reference?.Column);
                string comparison = leftEvaluable
                    ? parameter + " " + op + " " + columnSql
                    : columnSql + " " + op + " " + parameter;

                if (binary.NodeType == ExpressionType.NotEqual && IsNullable(columnSide, reference))
                    return "(" + comparison + " OR " + columnSql + " IS NULL)";
                return "(" + comparison + ")";
            }

            string leftSql = Value(left);
            string rightSql = Value(right);
            bool leftNullable = !IsPredicateNode(UnwrapConvert(left)) && IsNullable(left, ResolveColumn(left));
            bool rightNullable = !IsPredicateNode(UnwrapConvert(right)) && IsNullable(right, ResolveColumn(right));
            string plain = "(" + leftSql + " " + op + " " + rightSql + ")";

            // C# semantics for nullable operands: null == null is true, null != value is true.
            if (binary.NodeType == ExpressionType.Equal && leftNullable && rightNullable)
                return "(" + plain + " OR (" + leftSql + " IS NULL AND " + rightSql + " IS NULL))";
            if (binary.NodeType == ExpressionType.NotEqual && (leftNullable || rightNullable))
            {
                List<string> terms = new List<string> { plain };
                if (leftNullable && rightNullable)
                {
                    terms.Add("(" + leftSql + " IS NULL AND " + rightSql + " IS NOT NULL)");
                    terms.Add("(" + leftSql + " IS NOT NULL AND " + rightSql + " IS NULL)");
                }
                else if (leftNullable)
                {
                    terms.Add(leftSql + " IS NULL");
                }
                else
                {
                    terms.Add(rightSql + " IS NULL");
                }

                return "(" + string.Join(" OR ", terms) + ")";
            }

            return plain;
        }

        private bool TryTranslateCompareTo(Expression call, Expression zero, ExpressionType nodeType, out string? sql)
        {
            sql = null;
            if (UnwrapConvert(call) is not MethodCallExpression method) return false;
            if (!ExpressionEvaluator.IsEvaluable(zero) || !(ExpressionEvaluator.Evaluate(zero) is int z) || z != 0) return false;

            Expression? a = null;
            Expression? b = null;
            if (method.Method.Name == "CompareTo" && method.Object != null && method.Arguments.Count == 1)
            {
                a = method.Object;
                b = method.Arguments[0];
            }
            else if (method.Method.Name == "Compare" && method.Method.DeclaringType == typeof(string) && method.Arguments.Count >= 2)
            {
                a = method.Arguments[0];
                b = method.Arguments[1];
            }

            if (a == null || b == null) return false;
            sql = TranslateComparison(Expression.MakeBinary(nodeType, a, b, false, CompareMethodFor(a.Type)));
            return true;
        }

        private static MethodInfo? CompareMethodFor(Type type)
        {
            if (type != typeof(string)) return null;
            return typeof(SqlExpressionTranslator).GetMethod(nameof(StringComparePlaceholder), BindingFlags.NonPublic | BindingFlags.Static);
        }

        private static bool StringComparePlaceholder(string a, string b)
        {
            return string.CompareOrdinal(a, b) == 0;
        }

        private static ExpressionType Flip(ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.LessThan: return ExpressionType.GreaterThan;
                case ExpressionType.LessThanOrEqual: return ExpressionType.GreaterThanOrEqual;
                case ExpressionType.GreaterThan: return ExpressionType.LessThan;
                case ExpressionType.GreaterThanOrEqual: return ExpressionType.LessThanOrEqual;
                default: return type;
            }
        }

        private static string ComparisonOperator(ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.Equal: return "=";
                case ExpressionType.NotEqual: return "<>";
                case ExpressionType.LessThan: return "<";
                case ExpressionType.LessThanOrEqual: return "<=";
                case ExpressionType.GreaterThan: return ">";
                case ExpressionType.GreaterThanOrEqual: return ">=";
                default: throw new NotSupportedException("Operator " + type + " is not a comparison.");
            }
        }

        private static bool IsNullable(Expression expression, ColumnReference? reference)
        {
            if (reference != null) return reference.Column.IsNullable;
            Type type = UnwrapConvert(expression).Type;
            return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
        }

        private string TranslateMember(MemberExpression member)
        {
            if (member.Expression == null)
                throw Unsupported(member);

            if (member.Expression is ParameterExpression parameter)
            {
                if (!_Sources.TryGetValue(parameter, out TableSource? source))
                    throw new NotSupportedException("Parameter '" + parameter.Name + "' is not bound to a table in this query.");
                if (member.Member is PropertyInfo property)
                {
                    ColumnMetadata? column = source.Metadata.FindColumn(property);
                    if (column != null) return ColumnSql(source, column);
                    if (source.Metadata.FindNavigation(property.Name) != null)
                        throw new NotSupportedException("Navigation '" + property.Name + "' cannot be used as a value; compare one of its members instead.");
                }

                throw new NotSupportedException("Member '" + member.Member.Name + "' of " + source.Metadata.EntityType.Name + " is not a mapped column.");
            }

            Type declaring = member.Expression.Type;
            Type? nullableUnderlying = Nullable.GetUnderlyingType(declaring);
            if (nullableUnderlying != null)
            {
                if (member.Member.Name == "HasValue") return "(" + Value(member.Expression) + " IS NOT NULL)";
                if (member.Member.Name == "Value") return Value(member.Expression);
            }

            if (declaring == typeof(string) && member.Member.Name == "Length")
                return Dialect.TranslateFunction(SqlFunction.Length, new[] { Value(member.Expression) });

            Type dateType = nullableUnderlying ?? declaring;
            if (dateType == typeof(DateTime) || dateType == typeof(DateTimeOffset) || dateType == typeof(DateOnly))
            {
                SqlFunction? part = member.Member.Name switch
                {
                    "Year" => SqlFunction.Year,
                    "Month" => SqlFunction.Month,
                    "Day" => SqlFunction.Day,
                    "Hour" => SqlFunction.Hour,
                    "Minute" => SqlFunction.Minute,
                    "Second" => SqlFunction.Second,
                    "DayOfYear" => SqlFunction.DayOfYear,
                    "DayOfWeek" => SqlFunction.DayOfWeek,
                    "Date" => SqlFunction.Date,
                    _ => null
                };
                if (part.HasValue) return Dialect.TranslateFunction(part.Value, new[] { Value(member.Expression) });
            }

            if (member.Member.Name == "Count" && TryGetCollectionNavigation(member.Expression, out NavigationMetadata? countNavigation, out Expression? countOwner))
                return "(" + CollectionSubquery(countNavigation!, countOwner!, null, "COUNT(*)") + ")";

            if (TryGetReferenceNavigation(member.Expression, out NavigationMetadata? navigation, out Expression? owner) && member.Member is PropertyInfo relatedProperty)
            {
                EntityMetadata related = EntityMetadata.For(navigation!.RelatedType);
                ColumnMetadata? relatedColumn = related.FindColumn(relatedProperty);
                if (relatedColumn == null)
                    throw new NotSupportedException("Member '" + relatedProperty.Name + "' of " + related.EntityType.Name + " is not a mapped column.");

                string localSql = Value(Expression.Property(owner!, navigation.LocalColumn.Property));
                string alias = Builder.NextAlias();
                TableSource relatedSource = new TableSource(alias, related);
                return "(SELECT " + ColumnSql(relatedSource, relatedColumn) + " FROM " + Dialect.QuoteIdentifier(related.TableName) + " " + alias
                    + " WHERE " + ColumnSql(relatedSource, navigation.RemoteColumn) + " = " + localSql + ")";
            }

            throw Unsupported(member);
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

        private string CollectionSubquery(NavigationMetadata navigation, Expression owner, LambdaExpression? predicate, string selectList, bool negatePredicate = false)
        {
            EntityMetadata related = EntityMetadata.For(navigation.RelatedType);
            string localSql = Value(Expression.Property(owner, navigation.LocalColumn.Property));
            string alias = Builder.NextAlias();
            TableSource relatedSource = new TableSource(alias, related);

            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.Append("SELECT ").Append(selectList).Append(" FROM ").Append(Dialect.QuoteIdentifier(related.TableName)).Append(' ').Append(alias);
            if (navigation.Kind == NavigationKind.Collection)
            {
                sql.Append(" WHERE ").Append(ColumnSql(relatedSource, navigation.RemoteColumn)).Append(" = ").Append(localSql);
            }
            else
            {
                EntityMetadata junction = EntityMetadata.For(navigation.JunctionType!);
                string junctionAlias = Builder.NextAlias("j");
                TableSource junctionSource = new TableSource(junctionAlias, junction);
                sql.Append(" INNER JOIN ").Append(Dialect.QuoteIdentifier(junction.TableName)).Append(' ').Append(junctionAlias)
                    .Append(" ON ").Append(ColumnSql(junctionSource, navigation.JunctionRemoteColumn!)).Append(" = ").Append(ColumnSql(relatedSource, navigation.RemoteColumn))
                    .Append(" WHERE ").Append(ColumnSql(junctionSource, navigation.JunctionLocalColumn!)).Append(" = ").Append(localSql);
            }

            string? softDelete = SoftDeleteFilter(relatedSource);
            if (softDelete != null) sql.Append(" AND ").Append(softDelete);

            if (predicate != null)
            {
                Bind(predicate.Parameters[0], relatedSource);
                string condition = Predicate(predicate.Body);
                sql.Append(" AND ").Append(negatePredicate ? "(NOT " + condition + ")" : condition);
            }

            return sql.ToString();
        }

        private string TranslateMethod(MethodCallExpression call)
        {
            MethodInfo method = call.Method;
            Type? declaring = method.DeclaringType;
            string name = method.Name;

            if (declaring == typeof(string))
            {
                string? stringSql = TranslateStringMethod(call);
                if (stringSql != null) return stringSql;
            }

            if (name == "Contains" && TryTranslateCollectionContains(call, out string? containsSql)) return containsSql!;

            if (declaring == typeof(Enumerable) && call.Arguments.Count >= 1 && TryGetCollectionNavigation(call.Arguments[0], out NavigationMetadata? navigation, out Expression? owner))
            {
                LambdaExpression? lambda = call.Arguments.Count > 1 ? (LambdaExpression)StripQuotes(call.Arguments[1]) : null;
                switch (name)
                {
                    case "Any":
                        return "EXISTS (" + CollectionSubquery(navigation!, owner!, lambda, "1") + ")";
                    case "All":
                        if (lambda == null) throw Unsupported(call);
                        return "NOT EXISTS (" + CollectionSubquery(navigation!, owner!, lambda, "1", true) + ")";
                    case "Count":
                    case "LongCount":
                        return "(" + CollectionSubquery(navigation!, owner!, lambda, "COUNT(*)") + ")";
                }
            }

            if (declaring == typeof(ExpressionExtensions))
            {
                switch (name)
                {
                    case "Between":
                        {
                            ColumnReference? reference = ResolveColumn(call.Arguments[0]);
                            string valueSql = Value(call.Arguments[0]);
                            return "(" + valueSql + " BETWEEN " + Operand(call.Arguments[1], reference) + " AND " + Operand(call.Arguments[2], reference) + ")";
                        }
                    case "In":
                    case "NotIn":
                        {
                            string inSql = TranslateIn(call.Arguments[0], call.Arguments[1], name == "NotIn");
                            return inSql;
                        }
                    case "IsNull":
                        return "(" + Value(call.Arguments[0]) + " IS NULL)";
                    case "IsNotNull":
                        return "(" + Value(call.Arguments[0]) + " IS NOT NULL)";
                }
            }

            if (declaring == typeof(Math) || declaring == typeof(decimal) && (name == "Round" || name == "Floor" || name == "Ceiling" || name == "Abs"))
            {
                List<string> arguments = call.Arguments.Where(a => a.Type != typeof(MidpointRounding)).Select(Value).ToList();
                switch (name)
                {
                    case "Abs": return Dialect.TranslateFunction(SqlFunction.Abs, arguments);
                    case "Round": return Dialect.TranslateFunction(SqlFunction.Round, arguments);
                    case "Ceiling": return Dialect.TranslateFunction(SqlFunction.Ceiling, arguments);
                    case "Floor": return Dialect.TranslateFunction(SqlFunction.Floor, arguments);
                    case "Pow": return Dialect.TranslateFunction(SqlFunction.Power, arguments);
                    case "Sqrt": return Dialect.TranslateFunction(SqlFunction.Sqrt, arguments);
                }
            }

            if (call.Object != null && (call.Object.Type == typeof(DateTime) || call.Object.Type == typeof(DateTimeOffset) || call.Object.Type == typeof(DateOnly)) && name.StartsWith("Add", StringComparison.Ordinal))
            {
                SqlFunction? function = name switch
                {
                    "AddYears" => SqlFunction.AddYears,
                    "AddMonths" => SqlFunction.AddMonths,
                    "AddDays" => SqlFunction.AddDays,
                    "AddHours" => SqlFunction.AddHours,
                    "AddMinutes" => SqlFunction.AddMinutes,
                    "AddSeconds" => SqlFunction.AddSeconds,
                    _ => null
                };
                if (function.HasValue)
                    return Dialect.TranslateFunction(function.Value, new[] { Value(call.Object), Value(call.Arguments[0]) });
            }

            if (name == "Equals")
            {
                if (call.Object != null && call.Arguments.Count == 1)
                    return TranslateComparison(Expression.Equal(UnwrapConvert(call.Object), ConvertTo(call.Arguments[0], UnwrapConvert(call.Object).Type)));
                if (call.Object == null && call.Arguments.Count == 2)
                    return TranslateComparison(Expression.Equal(UnwrapConvert(call.Arguments[0]), ConvertTo(call.Arguments[1], UnwrapConvert(call.Arguments[0]).Type)));
            }

            throw Unsupported(call);
        }

        private static Expression ConvertTo(Expression expression, Type type)
        {
            Expression unwrapped = UnwrapConvert(expression);
            if (unwrapped.Type == type) return unwrapped;
            return Expression.Convert(unwrapped, type);
        }

        private string Operand(Expression expression, ColumnReference? reference)
        {
            if (ExpressionEvaluator.IsEvaluable(expression))
                return Parameter(ExpressionEvaluator.Evaluate(expression), reference?.Column);
            return Value(expression);
        }

        private string? TranslateStringMethod(MethodCallExpression call)
        {
            string name = call.Method.Name;
            Expression? instance = call.Object;

            if (instance == null)
            {
                switch (name)
                {
                    case "IsNullOrEmpty":
                        {
                            string sql = Value(call.Arguments[0]);
                            return "(" + sql + " IS NULL OR " + Dialect.IsEmptyString(sql) + ")";
                        }
                    case "IsNullOrWhiteSpace":
                        {
                            string sql = Value(call.Arguments[0]);
                            return "(" + sql + " IS NULL OR " + Dialect.IsEmptyString(Dialect.TranslateFunction(SqlFunction.Trim, new[] { sql })) + ")";
                        }
                    case "Concat":
                        if (call.Arguments.Count == 1 && call.Arguments[0] is NewArrayExpression array)
                            return Dialect.Concat(array.Expressions.Select(StringOperand).ToList());
                        return Dialect.Concat(call.Arguments.Select(StringOperand).ToList());
                    case "Equals":
                        if (call.Arguments.Count == 3 && IsIgnoreCase(call.Arguments[2]))
                            return "(" + Lower(call.Arguments[0]) + " = " + Lower(call.Arguments[1]) + ")";
                        return null;
                    case "Compare":
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
                        bool ignoreCase = call.Arguments.Count > 1 && IsIgnoreCase(call.Arguments[call.Arguments.Count - 1]);
                        string target = ignoreCase ? Lower(instance) : Value(instance);
                        string pattern;
                        if (ExpressionEvaluator.IsEvaluable(argument))
                        {
                            object? raw = ExpressionEvaluator.Evaluate(argument);
                            if (raw == null) return "(1 = 0)";
                            string text = Convert.ToString(raw, CultureInfo.InvariantCulture)!;
                            if (ignoreCase) text = text.ToLowerInvariant();
                            string escaped = Dialect.EscapeLikePattern(text);
                            string like = name == "Contains" ? "%" + escaped + "%" : name == "StartsWith" ? escaped + "%" : "%" + escaped;
                            pattern = Builder.AddParameter(like);
                        }
                        else
                        {
                            string valueSql = ignoreCase ? Lower(argument) : Value(argument);
                            List<string> parts = new List<string>();
                            if (name != "StartsWith") parts.Add("'%'");
                            parts.Add(valueSql);
                            if (name != "EndsWith") parts.Add("'%'");
                            pattern = Dialect.Concat(parts);
                        }

                        return "(" + target + " LIKE " + pattern + " ESCAPE '" + Dialect.LikeEscapeCharacter + "')";
                    }
                case "Equals":
                    if (call.Arguments.Count == 2 && IsIgnoreCase(call.Arguments[1]))
                        return "(" + Lower(instance) + " = " + Lower(call.Arguments[0]) + ")";
                    return null;
                case "ToUpper":
                case "ToUpperInvariant":
                    return Dialect.TranslateFunction(SqlFunction.Upper, new[] { Value(instance) });
                case "ToLower":
                case "ToLowerInvariant":
                    return Dialect.TranslateFunction(SqlFunction.Lower, new[] { Value(instance) });
                case "Trim":
                    if (call.Arguments.Count > 0) return null;
                    return Dialect.TranslateFunction(SqlFunction.Trim, new[] { Value(instance) });
                case "TrimStart":
                    if (call.Arguments.Count > 0 && !IsEmptyCharArray(call.Arguments[0])) return null;
                    return Dialect.TranslateFunction(SqlFunction.TrimStart, new[] { Value(instance) });
                case "TrimEnd":
                    if (call.Arguments.Count > 0 && !IsEmptyCharArray(call.Arguments[0])) return null;
                    return Dialect.TranslateFunction(SqlFunction.TrimEnd, new[] { Value(instance) });
                case "Substring":
                    {
                        List<string> arguments = new List<string> { Value(instance) };
                        arguments.AddRange(call.Arguments.Select(Value));
                        return Dialect.TranslateFunction(SqlFunction.Substring, arguments);
                    }
                case "Replace":
                    if (call.Arguments.Count != 2 || call.Arguments[0].Type != typeof(string)) return null;
                    return Dialect.TranslateFunction(SqlFunction.Replace, new[] { Value(instance), Value(call.Arguments[0]), Value(call.Arguments[1]) });
                case "IndexOf":
                    if (call.Arguments.Count != 1 || call.Arguments[0].Type != typeof(string)) return null;
                    return Dialect.TranslateFunction(SqlFunction.IndexOf, new[] { Value(instance), Value(call.Arguments[0]) });
            }

            return null;
        }

        private static bool IsEmptyCharArray(Expression expression)
        {
            if (!ExpressionEvaluator.IsEvaluable(expression)) return false;
            object? value = ExpressionEvaluator.Evaluate(expression);
            return value == null || (value is char[] chars && chars.Length == 0);
        }

        private string Lower(Expression expression)
        {
            if (ExpressionEvaluator.IsEvaluable(expression))
            {
                object? value = ExpressionEvaluator.Evaluate(expression);
                return Builder.AddParameter(value == null ? null : Convert.ToString(value, CultureInfo.InvariantCulture)!.ToLowerInvariant());
            }

            return Dialect.TranslateFunction(SqlFunction.Lower, new[] { Value(expression) });
        }

        private static bool IsIgnoreCase(Expression expression)
        {
            if (expression.Type != typeof(StringComparison) || !ExpressionEvaluator.IsEvaluable(expression)) return false;
            StringComparison comparison = (StringComparison)ExpressionEvaluator.Evaluate(expression)!;
            return comparison == StringComparison.OrdinalIgnoreCase
                || comparison == StringComparison.CurrentCultureIgnoreCase
                || comparison == StringComparison.InvariantCultureIgnoreCase;
        }

        private bool TryTranslateCollectionContains(MethodCallExpression call, out string? sql)
        {
            sql = null;
            Expression? collection;
            Expression? item;
            if (call.Object == null && call.Arguments.Count == 2)
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
                return false;
            }

            if (collection.Type == typeof(string)) return false;
            if (!ExpressionEvaluator.IsEvaluable(collection))
            {
                if (TryGetCollectionNavigation(collection, out _, out _))
                    throw new NotSupportedException("Contains on a navigation collection is not supported; use Any(x => ...) instead.");
                return false;
            }

            sql = TranslateIn(item, collection, false);
            return true;
        }

        private string TranslateIn(Expression item, Expression collection, bool negate)
        {
            if (!ExpressionEvaluator.IsEvaluable(collection))
                throw new NotSupportedException("The collection in an IN expression must be a client-side value.");

            object? source = ExpressionEvaluator.Evaluate(collection);
            List<object?> values = new List<object?>();
            if (source is IEnumerable enumerable && source is not string)
            {
                foreach (object? value in enumerable) values.Add(value);
            }
            else if (source != null)
            {
                values.Add(source);
            }

            if (values.Count == 0) return negate ? "(1 = 1)" : "(1 = 0)";

            ColumnReference? reference = ResolveColumn(item);
            string itemSql = Value(item);
            bool hasNull = values.Any(v => v == null);
            List<object?> nonNull = values.Where(v => v != null).Distinct().ToList();

            List<string> placeholders = new List<string>(nonNull.Count);
            bool inline = nonNull.Count > InlineIntegerListThreshold && nonNull.All(IsIntegral) && (reference == null || !reference.Column.IsEnum);
            foreach (object? value in nonNull)
            {
                placeholders.Add(inline
                    ? Convert.ToString(value, CultureInfo.InvariantCulture)!
                    : Parameter(value, reference?.Column));
            }

            string condition = nonNull.Count == 0 ? string.Empty : itemSql + (negate ? " NOT IN (" : " IN (") + string.Join(", ", placeholders) + ")";
            if (hasNull)
            {
                string nullCheck = itemSql + (negate ? " IS NOT NULL" : " IS NULL");
                condition = condition.Length == 0 ? nullCheck : condition + (negate ? " AND " : " OR ") + nullCheck;
            }
            else if (negate && IsNullable(item, reference))
            {
                condition = condition + " OR " + itemSql + " IS NULL";
            }

            return "(" + condition + ")";
        }

        private bool MayBeUnknown(Expression expression)
        {
            expression = UnwrapConvert(StripQuotes(expression));
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.And:
                case ExpressionType.Or:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        return MayBeUnknown(binary.Left) || MayBeUnknown(binary.Right);
                    }
                case ExpressionType.Not:
                    return MayBeUnknown(((UnaryExpression)expression).Operand);
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                    {
                        BinaryExpression binary = (BinaryExpression)expression;
                        bool leftEvaluable = ExpressionEvaluator.IsEvaluable(binary.Left);
                        bool rightEvaluable = ExpressionEvaluator.IsEvaluable(binary.Right);
                        if (expression.NodeType == ExpressionType.Equal || expression.NodeType == ExpressionType.NotEqual)
                        {
                            if ((leftEvaluable && ExpressionEvaluator.Evaluate(binary.Left) == null) || (rightEvaluable && ExpressionEvaluator.Evaluate(binary.Right) == null))
                                return false;
                            if (expression.NodeType == ExpressionType.NotEqual) return false;
                        }

                        return (!leftEvaluable && IsNullable(binary.Left, ResolveColumn(binary.Left)))
                            || (!rightEvaluable && IsNullable(binary.Right, ResolveColumn(binary.Right)));
                    }
                case ExpressionType.Call:
                    {
                        MethodCallExpression call = (MethodCallExpression)expression;
                        if (call.Method.Name == "IsNullOrEmpty" || call.Method.Name == "IsNullOrWhiteSpace" || call.Method.Name == "Any" || call.Method.Name == "All") return false;
                        if (call.Object != null && !ExpressionEvaluator.IsEvaluable(call.Object) && IsNullable(call.Object, ResolveColumn(call.Object))) return true;
                        foreach (Expression argument in call.Arguments)
                        {
                            if (!ExpressionEvaluator.IsEvaluable(argument) && argument.Type != typeof(bool) && IsNullable(argument, ResolveColumn(argument))) return true;
                        }

                        return false;
                    }
                case ExpressionType.MemberAccess:
                    return expression.Type == typeof(bool?);
                default:
                    return false;
            }
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

        private object ConvertForColumn(object? value, ColumnMetadata? column)
        {
            if (value != null && column != null && column.IsEnum && column.Converter == null && !value.GetType().IsEnum && IsIntegral(value))
                value = Enum.ToObject(column.ClrType, value);
            return Converter.ConvertToDatabase(value, column);
        }

        private static bool IsPredicateNode(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    return true;
                case ExpressionType.And:
                case ExpressionType.Or:
                case ExpressionType.Not:
                    return expression.Type == typeof(bool) || expression.Type == typeof(bool?);
                case ExpressionType.MemberAccess:
                    {
                        MemberExpression member = (MemberExpression)expression;
                        return member.Member.Name == "HasValue" && member.Expression != null && Nullable.GetUnderlyingType(member.Expression.Type) != null;
                    }
                case ExpressionType.Call:
                    {
                        MethodCallExpression call = (MethodCallExpression)expression;
                        if (call.Type != typeof(bool)) return false;
                        string name = call.Method.Name;
                        return name == "Contains" || name == "StartsWith" || name == "EndsWith" || name == "Equals"
                            || name == "IsNullOrEmpty" || name == "IsNullOrWhiteSpace" || name == "Any" || name == "All"
                            || name == "Between" || name == "In" || name == "NotIn" || name == "IsNull" || name == "IsNotNull";
                    }
                default:
                    return false;
            }
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
            return new NotSupportedException("The expression '" + expression + "' (" + expression.NodeType + ") cannot be translated to SQL. Evaluate it on the client or use a raw SQL predicate.");
        }

        #endregion
    }
}
