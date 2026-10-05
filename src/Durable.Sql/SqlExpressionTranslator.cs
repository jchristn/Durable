namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Renders LINQ expressions as SQL for any <see cref="ISqlDialect"/>. Expressions are first converted to the
    /// backend-neutral <see cref="QueryNode"/> tree by a <see cref="QueryNormalizer"/> (closure evaluation, enum and null
    /// handling, navigation and grouping analysis), then rendered here. Every client-side value is bound as a parameter
    /// (converted with the target column's rules when compared with a column), so the generated SQL is injection-safe,
    /// culture-invariant and plan-cache friendly. C# null semantics are preserved: <c>a != b</c> is true when exactly one
    /// side is null, and negating a comparison with null yields true. String comparisons honour the node's
    /// <see cref="StringMatchMode"/> through <see cref="ISqlDialect.OrdinalCollation"/>.
    /// Thread safety: not thread-safe; create one per statement.
    /// </summary>
    public class SqlExpressionTranslator : QueryNodeVisitor<string>
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
        /// Gets the normalizer that converts expressions to query nodes. Never null.
        /// </summary>
        public QueryNormalizer Normalizer { get; }

        /// <summary>
        /// Gets or sets the number of distinct integer values above which an <c>IN</c> list is written as literals instead of
        /// parameters (to stay below driver parameter limits). Integers are culture-invariant and injection-safe.
        /// Default: 500. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
        public int InlineIntegerListThreshold
        {
            get => _InlineIntegerListThreshold;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "InlineIntegerListThreshold must be at least 1.");
                _InlineIntegerListThreshold = value;
            }
        }

        #endregion

        #region Private-Members

        private int _InlineIntegerListThreshold = 500;
        private readonly Dictionary<QuerySource, TableSource> _Tables = new Dictionary<QuerySource, TableSource>();
        private readonly Dictionary<TableSource, QuerySource> _Sources = new Dictionary<TableSource, QuerySource>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a translator.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="converter">Converter; null uses the dialect's.</param>
        /// <param name="stringMatching">Mode for string comparisons without an explicit <see cref="StringComparison"/>.</param>
        /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
        public SqlExpressionTranslator(SqlStatementBuilder builder, IDataTypeConverter? converter = null, StringMatchMode stringMatching = StringMatchMode.Database)
        {
            Builder = builder ?? throw new ArgumentNullException(nameof(builder));
            Converter = converter ?? builder.Dialect.Converter;
            Normalizer = new QueryNormalizer(stringMatching);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Binds a lambda parameter to a table.
        /// </summary>
        /// <param name="parameter">Lambda parameter. Must not be null.</param>
        /// <param name="source">Table source. Must not be null.</param>
        /// <returns>This translator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SqlExpressionTranslator Bind(ParameterExpression parameter, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(source);
            Normalizer.Bind(parameter, QueryFor(source));
            return this;
        }

        /// <summary>
        /// Returns the query source representing a table, creating it on first use.
        /// </summary>
        /// <param name="table">Table source. Must not be null.</param>
        /// <returns>The query source.</returns>
        /// <exception cref="ArgumentNullException">Thrown when table is null.</exception>
        public QuerySource QueryFor(TableSource table)
        {
            ArgumentNullException.ThrowIfNull(table);
            if (_Sources.TryGetValue(table, out QuerySource? existing)) return existing;
            QuerySource source = new QuerySource(table.Metadata, table.Qualifier);
            _Sources[table] = source;
            _Tables[source] = table;
            return source;
        }

        /// <summary>
        /// Activates grouping translation (<c>g.Key</c>, <c>g.Count()</c>, <c>g.Sum(...)</c>, ...) over rows of a table.
        /// </summary>
        /// <param name="grouping">Grouping. Must not be null.</param>
        /// <param name="source">Table the grouped rows come from. Must not be null.</param>
        /// <returns>This translator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SqlExpressionTranslator UseGrouping(GroupingSpecification grouping, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(grouping);
            ArgumentNullException.ThrowIfNull(source);
            Normalizer.UseGrouping(grouping, QueryFor(source));
            return this;
        }

        /// <summary>
        /// Renders the key parts of a grouping (the GROUP BY list).
        /// </summary>
        /// <param name="grouping">Grouping. Must not be null.</param>
        /// <param name="source">Table the grouped rows come from. Must not be null.</param>
        /// <returns>One SQL expression per key part.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public List<string> GroupKeySql(GroupingSpecification grouping, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(grouping);
            ArgumentNullException.ThrowIfNull(source);
            return Normalizer.NormalizeGroupKey(grouping, QueryFor(source)).Select(Value).ToList();
        }

        /// <summary>
        /// Translates a lambda body as a boolean condition, binding its single parameter to the source.
        /// </summary>
        /// <param name="lambda">Predicate lambda. Must not be null.</param>
        /// <param name="source">Source for the lambda parameter. Must not be null.</param>
        /// <returns>SQL condition.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for untranslatable constructs.</exception>
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
        /// <exception cref="ArgumentNullException">Thrown when expression is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for untranslatable constructs.</exception>
        public string Predicate(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            return Predicate(Normalizer.Normalize(expression));
        }

        /// <summary>
        /// Translates an expression as a value (column, parameter, function or computed expression).
        /// Boolean conditions in value position become CASE expressions.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>SQL value expression.</returns>
        /// <exception cref="ArgumentNullException">Thrown when expression is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for untranslatable constructs.</exception>
        public string Value(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            return Value(Normalizer.Normalize(expression));
        }

        /// <summary>
        /// Renders a node as a boolean condition. Values are compared with the dialect's true literal; constant booleans
        /// become <c>(1 = 1)</c> or <c>(1 = 0)</c>.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>SQL condition.</returns>
        /// <exception cref="ArgumentNullException">Thrown when node is null.</exception>
        public string Predicate(QueryNode node)
        {
            ArgumentNullException.ThrowIfNull(node);
            if (node is ValueNode constant) return constant.Value is bool b && b ? "(1 = 1)" : "(1 = 0)";
            if (IsPredicateNode(node)) return Visit(node);
            return "(" + Value(node) + " = " + Dialect.BooleanLiteral(true) + ")";
        }

        /// <summary>
        /// Renders a node as a value. Conditions in value position become CASE expressions.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>SQL value expression.</returns>
        /// <exception cref="ArgumentNullException">Thrown when node is null.</exception>
        public string Value(QueryNode node)
        {
            ArgumentNullException.ThrowIfNull(node);
            if (node is ValueNode) return Visit(node);
            if (IsPredicateNode(node))
                return "CASE WHEN " + Visit(node) + " THEN " + Dialect.BooleanLiteral(true) + " ELSE " + Dialect.BooleanLiteral(false) + " END";
            return Visit(node);
        }

        /// <summary>
        /// Resolves an expression to a mapped column when it is a direct member access on a bound parameter
        /// (ignoring conversions and Nullable.Value).
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>The column and its source, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when expression is null.</exception>
        public ColumnReference? ResolveColumn(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ColumnNode? column = Normalizer.ResolveColumn(expression);
            if (column == null) return null;
            return new ColumnReference(TableFor(column.Source), column.Column);
        }

        /// <summary>
        /// Returns the qualified SQL for a column.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>Qualified, quoted column reference.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
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
        /// <exception cref="ArgumentNullException">Thrown when source is null.</exception>
        public string? SoftDeleteFilter(TableSource source)
        {
            ArgumentNullException.ThrowIfNull(source);
            ColumnMetadata? column = source.Metadata.SoftDeleteColumn;
            if (column == null) return null;
            string sql = ColumnSql(source, column);
            if (column.ClrType == typeof(bool)) return "(" + sql + " = " + Dialect.BooleanLiteral(false) + " OR " + sql + " IS NULL)";
            return sql + " IS NULL";
        }

        /// <inheritdoc />
        public override string VisitColumn(ColumnNode node)
        {
            return ColumnSql(TableFor(node.Source), node.Column);
        }

        /// <inheritdoc />
        public override string VisitValue(ValueNode node)
        {
            return Parameter(node.Value, node.Column);
        }

        /// <inheritdoc />
        public override string VisitLogical(LogicalNode node)
        {
            string connective = node.Operator == LogicalOperator.And ? " AND " : " OR ";
            return "(" + Predicate(node.Left) + connective + Predicate(node.Right) + ")";
        }

        /// <inheritdoc />
        public override string VisitNot(NotNode node)
        {
            string inner = Predicate(node.Operand);
            // SQL three-valued logic would drop rows where the operand is UNKNOWN (a NULL compared);
            // C# evaluates such comparisons to false, so the negation is true.
            if (MayBeUnknown(node.Operand))
                return "(CASE WHEN " + inner + " THEN " + Dialect.BooleanLiteral(false) + " ELSE " + Dialect.BooleanLiteral(true) + " END = " + Dialect.BooleanLiteral(true) + ")";
            return "(NOT " + inner + ")";
        }

        /// <inheritdoc />
        public override string VisitNullCheck(NullCheckNode node)
        {
            return "(" + Value(node.Operand) + (node.IsNull ? " IS NULL)" : " IS NOT NULL)");
        }

        /// <inheritdoc />
        public override string VisitComparison(ComparisonNode node)
        {
            string op = ComparisonOperatorSql(node.Operator);
            StringMatchMode mode = node.Left.ClrType == typeof(string) || node.Right.ClrType == typeof(string) ? node.StringMode : StringMatchMode.Database;

            if (node.Left is ValueNode || node.Right is ValueNode)
            {
                bool valueOnLeft = node.Left is ValueNode;
                ValueNode value = (ValueNode)(valueOnLeft ? node.Left : node.Right);
                QueryNode other = valueOnLeft ? node.Right : node.Left;
                string otherSql = Value(other);
                string compared = ApplyStringMode(otherSql, mode);
                string parameter = StringModeParameter(value.Value, value.Column, mode);
                string comparison = valueOnLeft
                    ? parameter + " " + op + " " + compared
                    : compared + " " + op + " " + parameter;

                if (node.Operator == ComparisonOperator.NotEqual && IsNullable(other))
                    return "(" + comparison + " OR " + otherSql + " IS NULL)";
                return "(" + comparison + ")";
            }

            string leftSql = Value(node.Left);
            string rightSql = Value(node.Right);
            string leftCompared = ApplyStringMode(leftSql, mode);
            string rightCompared = mode == StringMatchMode.IgnoreCase ? Lower(rightSql) : rightSql;
            bool leftNullable = !IsPredicateNode(node.Left) && IsNullable(node.Left);
            bool rightNullable = !IsPredicateNode(node.Right) && IsNullable(node.Right);
            string plain = "(" + leftCompared + " " + op + " " + rightCompared + ")";

            // C# semantics for nullable operands: null == null is true, null != value is true.
            if (node.Operator == ComparisonOperator.Equal && leftNullable && rightNullable)
                return "(" + plain + " OR (" + leftSql + " IS NULL AND " + rightSql + " IS NULL))";
            if (node.Operator == ComparisonOperator.NotEqual && (leftNullable || rightNullable))
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

        /// <inheritdoc />
        public override string VisitIn(InNode node)
        {
            if (node.Values.Count == 0) return node.Negated ? "(1 = 1)" : "(1 = 0)";

            StringMatchMode mode = node.Item.ClrType == typeof(string) ? node.StringMode : StringMatchMode.Database;
            ColumnMetadata? column = (node.Item as ColumnNode)?.Column;
            string itemSql = Value(node.Item);
            string compared = ApplyStringMode(itemSql, mode);
            bool hasNull = node.Values.Any(v => v == null);
            List<object?> nonNull = node.Values
                .Where(v => v != null)
                .Select(v => mode == StringMatchMode.IgnoreCase && v is string s ? s.ToLowerInvariant() : v)
                .Distinct()
                .ToList();

            List<string> placeholders = new List<string>(nonNull.Count);
            bool inline = nonNull.Count > InlineIntegerListThreshold && nonNull.All(IsIntegral) && (column == null || !column.IsEnum);
            foreach (object? value in nonNull)
            {
                placeholders.Add(inline
                    ? Convert.ToString(value, CultureInfo.InvariantCulture)!
                    : Parameter(value, column));
            }

            string condition = nonNull.Count == 0 ? string.Empty : compared + (node.Negated ? " NOT IN (" : " IN (") + string.Join(", ", placeholders) + ")";
            if (hasNull)
            {
                string nullCheck = itemSql + (node.Negated ? " IS NOT NULL" : " IS NULL");
                condition = condition.Length == 0 ? nullCheck : condition + (node.Negated ? " AND " : " OR ") + nullCheck;
            }
            else if (node.Negated && IsNullable(node.Item))
            {
                condition = condition + " OR " + itemSql + " IS NULL";
            }

            return "(" + condition + ")";
        }

        /// <inheritdoc />
        public override string VisitStringMatch(StringMatchNode node)
        {
            string target = Value(node.Target);

            if (node.Mode == StringMatchMode.Ordinal && !Dialect.SupportsOrdinalLike)
            {
                string valueSql = node.Pattern is ValueNode literal ? Builder.AddParameter(literal.Value) : Value(node.Pattern);
                return Dialect.OrdinalStringMatch(node.Kind, target, valueSql);
            }

            string targetSql = ApplyStringMode(target, node.Mode);
            string pattern;
            if (node.Pattern is ValueNode value)
            {
                string text = Convert.ToString(value.Value, CultureInfo.InvariantCulture) ?? string.Empty;
                if (node.Mode == StringMatchMode.IgnoreCase) text = text.ToLowerInvariant();
                string escaped = Dialect.EscapeLikePattern(text);
                string like = node.Kind == StringMatchKind.Contains ? "%" + escaped + "%" : node.Kind == StringMatchKind.StartsWith ? escaped + "%" : "%" + escaped;
                pattern = Builder.AddParameter(like);
            }
            else
            {
                string valueSql = Value(node.Pattern);
                if (node.Mode == StringMatchMode.IgnoreCase) valueSql = Lower(valueSql);
                List<string> parts = new List<string>();
                if (node.Kind != StringMatchKind.StartsWith) parts.Add("'%'");
                parts.Add(valueSql);
                if (node.Kind != StringMatchKind.EndsWith) parts.Add("'%'");
                pattern = Dialect.Concat(parts);
            }

            return "(" + targetSql + " LIKE " + pattern + " ESCAPE '" + Dialect.LikeEscapeCharacter + "')";
        }

        /// <inheritdoc />
        public override string VisitStringTest(StringTestNode node)
        {
            string sql = Value(node.Operand);
            if (node.Kind == StringTestKind.IsNullOrEmpty)
                return "(" + sql + " IS NULL OR " + Dialect.IsEmptyString(sql) + ")";
            return "(" + sql + " IS NULL OR " + Dialect.IsEmptyString(Dialect.TranslateFunction(QueryFunction.Trim, new[] { sql })) + ")";
        }

        /// <inheritdoc />
        public override string VisitArithmetic(ArithmeticNode node)
        {
            string left = Value(node.Left);
            string right = Value(node.Right);
            switch (node.Operator)
            {
                case ArithmeticOperator.Add: return "(" + left + " + " + right + ")";
                case ArithmeticOperator.Subtract: return "(" + left + " - " + right + ")";
                case ArithmeticOperator.Multiply: return "(" + left + " * " + right + ")";
                case ArithmeticOperator.Divide: return Dialect.Divide(left, right, node.IntegerDivision);
                default: return "(" + left + " % " + right + ")";
            }
        }

        /// <inheritdoc />
        public override string VisitNegate(NegateNode node)
        {
            return "(-" + Value(node.Operand) + ")";
        }

        /// <inheritdoc />
        public override string VisitConcat(ConcatNode node)
        {
            List<string> parts = new List<string>(node.Parts.Count);
            foreach (QueryNode part in node.Parts)
            {
                if (part is ValueNode value)
                {
                    parts.Add(Builder.AddParameter(value.Value ?? string.Empty));
                    continue;
                }

                string sql = Value(part);
                parts.Add(part.ClrType != typeof(string) ? "CAST(" + sql + " AS " + Dialect.StringCastType + ")" : "COALESCE(" + sql + ", '')");
            }

            return Dialect.Concat(parts);
        }

        /// <inheritdoc />
        public override string VisitCoalesce(CoalesceNode node)
        {
            return "COALESCE(" + Value(node.Left) + ", " + Value(node.Right) + ")";
        }

        /// <inheritdoc />
        public override string VisitConditional(ConditionalNode node)
        {
            return "CASE WHEN " + Predicate(node.Test) + " THEN " + Value(node.IfTrue) + " ELSE " + Value(node.IfFalse) + " END";
        }

        /// <inheritdoc />
        public override string VisitFunction(FunctionNode node)
        {
            List<string> arguments = node.Arguments.Select(Value).ToList();
            if (node.Function == QueryFunction.IndexOf && node.StringMode != StringMatchMode.Database)
            {
                if (node.StringMode == StringMatchMode.IgnoreCase)
                {
                    arguments[0] = Dialect.OrdinalCollation(Lower(arguments[0]));
                    arguments[1] = Lower(arguments[1]);
                }
                else
                {
                    arguments[0] = Dialect.OrdinalCollation(arguments[0]);
                }
            }
            else if (node.Function == QueryFunction.Replace && node.StringMode != StringMatchMode.Database)
            {
                // string.Replace(string, string) is ordinal in C#; an ignore-case default cannot apply without changing
                // the replaced text, so Replace is ordinal under both non-database modes.
                arguments[0] = Dialect.OrdinalCollation(arguments[0]);
            }

            return Dialect.TranslateFunction(node.Function, arguments);
        }

        /// <inheritdoc />
        public override string VisitNavigationMember(NavigationMemberNode node)
        {
            string localSql = Value(node.OwnerKey);
            TableSource related = Register(node.RelatedSource);
            string? softDelete = SoftDeleteFilter(related);
            return "(SELECT " + ColumnSql(related, node.Column) + " FROM " + Dialect.QuoteIdentifier(related.Metadata.TableName) + " " + related.Qualifier
                + " WHERE " + ColumnSql(related, node.Navigation.RemoteColumn) + " = " + localSql + (softDelete == null ? string.Empty : " AND " + softDelete) + ")";
        }

        /// <inheritdoc />
        public override string VisitCollection(CollectionNode node)
        {
            switch (node.Operation)
            {
                case CollectionOperation.Any:
                    return "EXISTS (" + CollectionSubquery(node, "1", false) + ")";
                case CollectionOperation.All:
                    return "NOT EXISTS (" + CollectionSubquery(node, "1", true) + ")";
                default:
                    return "(" + CollectionSubquery(node, "COUNT(*)", false) + ")";
            }
        }

        /// <inheritdoc />
        public override string VisitAggregate(AggregateNode node)
        {
            switch (node.Function)
            {
                case AggregateFunction.Count:
                    if (node.Operand == null) return "COUNT(*)";
                    return "SUM(CASE WHEN " + Predicate(node.Operand) + " THEN 1 ELSE 0 END)";
                case AggregateFunction.Any:
                    if (node.Operand == null) return "(COUNT(*) > 0)";
                    return "(SUM(CASE WHEN " + Predicate(node.Operand) + " THEN 1 ELSE 0 END) > 0)";
                case AggregateFunction.Sum:
                    return "COALESCE(SUM(" + Value(node.Operand!) + "), 0)";
                case AggregateFunction.Average:
                    return "AVG(CAST(" + Value(node.Operand!) + " AS DECIMAL(38, 10)))";
                case AggregateFunction.Min:
                    return "MIN(" + Value(node.Operand!) + ")";
                default:
                    return "MAX(" + Value(node.Operand!) + ")";
            }
        }

        #endregion

        #region Private-Methods

        private TableSource TableFor(QuerySource source)
        {
            if (_Tables.TryGetValue(source, out TableSource? table)) return table;
            throw new InvalidOperationException("Query source '" + source + "' is not bound to a table in this statement.");
        }

        private TableSource Register(QuerySource source)
        {
            TableSource table = new TableSource(Builder.NextAlias(), source.Metadata);
            _Tables[source] = table;
            _Sources[table] = source;
            return table;
        }

        private string CollectionSubquery(CollectionNode node, string selectList, bool negatePredicate)
        {
            NavigationMetadata navigation = node.Navigation;
            string localSql = Value(node.OwnerKey);
            TableSource related = Register(node.RelatedSource);

            StringBuilder sql = new StringBuilder();
            sql.Append("SELECT ").Append(selectList).Append(" FROM ").Append(Dialect.QuoteIdentifier(related.Metadata.TableName)).Append(' ').Append(related.Qualifier);
            if (navigation.Kind == NavigationKind.Collection)
            {
                sql.Append(" WHERE ").Append(ColumnSql(related, navigation.RemoteColumn)).Append(" = ").Append(localSql);
            }
            else
            {
                EntityMetadata junction = EntityMetadata.For(navigation.JunctionType!);
                string junctionAlias = Builder.NextAlias("j");
                TableSource junctionSource = new TableSource(junctionAlias, junction);
                sql.Append(" INNER JOIN ").Append(Dialect.QuoteIdentifier(junction.TableName)).Append(' ').Append(junctionAlias)
                    .Append(" ON ").Append(ColumnSql(junctionSource, navigation.JunctionRemoteColumn!)).Append(" = ").Append(ColumnSql(related, navigation.RemoteColumn))
                    .Append(" WHERE ").Append(ColumnSql(junctionSource, navigation.JunctionLocalColumn!)).Append(" = ").Append(localSql);
            }

            string? softDelete = SoftDeleteFilter(related);
            if (softDelete != null) sql.Append(" AND ").Append(softDelete);

            if (node.Predicate != null)
            {
                string condition = Predicate(node.Predicate);
                if (!negatePredicate)
                {
                    sql.Append(" AND ").Append(condition);
                }
                else if (MayBeUnknown(node.Predicate))
                {
                    // All(): a related row whose condition is UNKNOWN (a NULL compared) is false in C#, so it violates All.
                    sql.Append(" AND (CASE WHEN ").Append(condition).Append(" THEN ").Append(Dialect.BooleanLiteral(false))
                        .Append(" ELSE ").Append(Dialect.BooleanLiteral(true)).Append(" END = ").Append(Dialect.BooleanLiteral(true)).Append(')');
                }
                else
                {
                    sql.Append(" AND (NOT ").Append(condition).Append(')');
                }
            }

            return sql.ToString();
        }

        private string ApplyStringMode(string sql, StringMatchMode mode)
        {
            switch (mode)
            {
                case StringMatchMode.Ordinal:
                    return Dialect.OrdinalCollation(sql);
                case StringMatchMode.IgnoreCase:
                    return Dialect.OrdinalCollation(Lower(sql));
                default:
                    return sql;
            }
        }

        private string StringModeParameter(object? value, ColumnMetadata? column, StringMatchMode mode)
        {
            if (mode == StringMatchMode.IgnoreCase && value is string text) value = text.ToLowerInvariant();
            return Parameter(value, column);
        }

        private string Lower(string sql)
        {
            return Dialect.TranslateFunction(QueryFunction.Lower, new[] { sql });
        }

        private static string ComparisonOperatorSql(ComparisonOperator op)
        {
            switch (op)
            {
                case ComparisonOperator.Equal: return "=";
                case ComparisonOperator.NotEqual: return "<>";
                case ComparisonOperator.LessThan: return "<";
                case ComparisonOperator.LessThanOrEqual: return "<=";
                case ComparisonOperator.GreaterThan: return ">";
                default: return ">=";
            }
        }

        private static bool IsNullable(QueryNode node)
        {
            if (node is ColumnNode column) return column.Column.IsNullable;
            if (node is ValueNode value) return value.Value == null;
            Type type = node.ClrType;
            return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
        }

        private static bool MayBeUnknown(QueryNode node)
        {
            switch (node)
            {
                case LogicalNode logical:
                    return MayBeUnknown(logical.Left) || MayBeUnknown(logical.Right);
                case NotNode not:
                    return MayBeUnknown(not.Operand);
                case ComparisonNode comparison:
                    if (comparison.Operator == ComparisonOperator.NotEqual) return false;
                    return (comparison.Left is not ValueNode && IsNullable(comparison.Left))
                        || (comparison.Right is not ValueNode && IsNullable(comparison.Right));
                case StringMatchNode match:
                    return (match.Target is not ValueNode && IsNullable(match.Target))
                        || (match.Pattern is not ValueNode && IsNullable(match.Pattern));
                case InNode membership:
                    return membership.Item is not ValueNode && IsNullable(membership.Item);
                case ColumnNode column:
                    return column.ClrType == typeof(bool?);
                default:
                    return false;
            }
        }

        private static bool IsPredicateNode(QueryNode node)
        {
            switch (node)
            {
                case ComparisonNode:
                case LogicalNode:
                case NotNode:
                case NullCheckNode:
                case InNode:
                case StringMatchNode:
                case StringTestNode:
                    return true;
                case CollectionNode collection:
                    return collection.Operation != CollectionOperation.Count;
                case AggregateNode aggregate:
                    return aggregate.Function == AggregateFunction.Any;
                default:
                    return false;
            }
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

        #endregion
    }
}
