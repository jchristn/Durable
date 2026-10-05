namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Durable;

    /// <summary>
    /// Renders a <see cref="QueryModel"/> or <see cref="QueryNode"/> as readable, SQL-like text for diagnostics, logs and
    /// <see cref="IQueryBuilder{T}.Query"/> on backends without a native query language, for example
    /// <c>FROM qt_items WHERE ((qt_items.price &gt; 10) AND (qt_items.name LIKE 'A%')) ORDER BY qt_items.name ASC TAKE 5</c>.
    /// Values are written inline (strings quoted) because the text is never executed.
    /// Thread safety: instances are stateless; safe for concurrent use.
    /// </summary>
    public class QueryModelFormatter : QueryNodeVisitor<string>
    {
        #region Public-Methods

        /// <summary>
        /// Describes a query model.
        /// </summary>
        /// <param name="model">Model. Must not be null.</param>
        /// <returns>The description. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when model is null.</exception>
        public static string Describe(QueryModel model)
        {
            ArgumentNullException.ThrowIfNull(model);
            QueryModelFormatter formatter = new QueryModelFormatter();
            StringBuilder text = new StringBuilder();
            text.Append(model.Distinct ? "SELECT DISTINCT * FROM " : "SELECT * FROM ").Append(model.Metadata.TableName);
            if (model.Filter != null) text.Append(" WHERE ").Append(formatter.Visit(model.Filter));
            if (model.Orderings.Count > 0)
                text.Append(" ORDER BY ").Append(string.Join(", ", model.Orderings.Select(o => formatter.Visit(o.Key) + (o.Descending ? " DESC" : " ASC"))));
            if (model.Skip.HasValue) text.Append(" SKIP ").Append(model.Skip.Value.ToString(CultureInfo.InvariantCulture));
            if (model.Take.HasValue) text.Append(" TAKE ").Append(model.Take.Value.ToString(CultureInfo.InvariantCulture));
            return text.ToString();
        }

        /// <summary>
        /// Describes a node.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The description. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when node is null.</exception>
        public static string Describe(QueryNode node)
        {
            ArgumentNullException.ThrowIfNull(node);
            return new QueryModelFormatter().Visit(node);
        }

        /// <inheritdoc />
        public override string VisitColumn(ColumnNode node)
        {
            return node.Source.Metadata.TableName + "." + node.Column.Name;
        }

        /// <inheritdoc />
        public override string VisitValue(ValueNode node)
        {
            return Literal(node.Value);
        }

        /// <inheritdoc />
        public override string VisitComparison(ComparisonNode node)
        {
            string op = node.Operator switch
            {
                ComparisonOperator.Equal => "=",
                ComparisonOperator.NotEqual => "<>",
                ComparisonOperator.LessThan => "<",
                ComparisonOperator.LessThanOrEqual => "<=",
                ComparisonOperator.GreaterThan => ">",
                _ => ">="
            };
            return "(" + Visit(node.Left) + " " + op + " " + Visit(node.Right) + Mode(node.StringMode) + ")";
        }

        /// <inheritdoc />
        public override string VisitLogical(LogicalNode node)
        {
            return "(" + Visit(node.Left) + (node.Operator == LogicalOperator.And ? " AND " : " OR ") + Visit(node.Right) + ")";
        }

        /// <inheritdoc />
        public override string VisitNot(NotNode node)
        {
            return "(NOT " + Visit(node.Operand) + ")";
        }

        /// <inheritdoc />
        public override string VisitNullCheck(NullCheckNode node)
        {
            return "(" + Visit(node.Operand) + (node.IsNull ? " IS NULL)" : " IS NOT NULL)");
        }

        /// <inheritdoc />
        public override string VisitIn(InNode node)
        {
            return "(" + Visit(node.Item) + (node.Negated ? " NOT IN (" : " IN (") + string.Join(", ", node.Values.Select(Literal)) + ")" + Mode(node.StringMode) + ")";
        }

        /// <inheritdoc />
        public override string VisitStringMatch(StringMatchNode node)
        {
            return "(" + Visit(node.Target) + " " + node.Kind.ToString().ToUpperInvariant() + " " + Visit(node.Pattern) + Mode(node.Mode) + ")";
        }

        /// <inheritdoc />
        public override string VisitStringTest(StringTestNode node)
        {
            return node.Kind.ToString().ToUpperInvariant() + "(" + Visit(node.Operand) + ")";
        }

        /// <inheritdoc />
        public override string VisitArithmetic(ArithmeticNode node)
        {
            string op = node.Operator switch
            {
                ArithmeticOperator.Add => "+",
                ArithmeticOperator.Subtract => "-",
                ArithmeticOperator.Multiply => "*",
                ArithmeticOperator.Divide => "/",
                _ => "%"
            };
            return "(" + Visit(node.Left) + " " + op + " " + Visit(node.Right) + ")";
        }

        /// <inheritdoc />
        public override string VisitNegate(NegateNode node)
        {
            return "(-" + Visit(node.Operand) + ")";
        }

        /// <inheritdoc />
        public override string VisitConcat(ConcatNode node)
        {
            return "CONCAT(" + string.Join(", ", node.Parts.Select(Visit)) + ")";
        }

        /// <inheritdoc />
        public override string VisitCoalesce(CoalesceNode node)
        {
            return "COALESCE(" + Visit(node.Left) + ", " + Visit(node.Right) + ")";
        }

        /// <inheritdoc />
        public override string VisitConditional(ConditionalNode node)
        {
            return "CASE WHEN " + Visit(node.Test) + " THEN " + Visit(node.IfTrue) + " ELSE " + Visit(node.IfFalse) + " END";
        }

        /// <inheritdoc />
        public override string VisitFunction(FunctionNode node)
        {
            return node.Function.ToString().ToUpperInvariant() + "(" + string.Join(", ", node.Arguments.Select(Visit)) + ")" + Mode(node.StringMode);
        }

        /// <inheritdoc />
        public override string VisitNavigationMember(NavigationMemberNode node)
        {
            return node.Navigation.Name + "." + node.Column.Name;
        }

        /// <inheritdoc />
        public override string VisitCollection(CollectionNode node)
        {
            string predicate = node.Predicate == null ? string.Empty : " WHERE " + Visit(node.Predicate);
            return node.Operation.ToString().ToUpperInvariant() + "(" + node.Navigation.Name + predicate + ")";
        }

        /// <inheritdoc />
        public override string VisitAggregate(AggregateNode node)
        {
            return node.Function.ToString().ToUpperInvariant() + "(" + (node.Operand == null ? "*" : Visit(node.Operand)) + ")";
        }

        #endregion

        #region Private-Methods

        private static string Mode(StringMatchMode mode)
        {
            return mode == StringMatchMode.Database ? string.Empty : " " + mode.ToString().ToUpperInvariant();
        }

        private static string Literal(object? value)
        {
            switch (value)
            {
                case null: return "NULL";
                case string text: return "'" + text.Replace("'", "''", StringComparison.Ordinal) + "'";
                case char c: return "'" + c + "'";
                case bool b: return b ? "TRUE" : "FALSE";
                case DateTime dateTime: return "'" + dateTime.ToString("o", CultureInfo.InvariantCulture) + "'";
                case DateTimeOffset offset: return "'" + offset.ToString("o", CultureInfo.InvariantCulture) + "'";
                case Enum e: return "'" + e + "'";
                case byte[] bytes: return "0x" + Convert.ToHexString(bytes);
                case IFormattable formattable: return formattable.ToString(null, CultureInfo.InvariantCulture);
                default: return "'" + value + "'";
            }
        }

        #endregion
    }
}
