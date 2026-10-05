namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Durable;

    /// <summary>
    /// Builds the condition nodes Durable adds to backend queries: primary-key matches, the soft-delete filter, AND
    /// combinations and plain column orderings.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class QueryConditions
    {
        #region Public-Methods

        /// <summary>
        /// Builds a condition matching one primary key (null key parts match null).
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="keyValues">One value per key column, in key order. Must not be null.</param>
        /// <returns>The condition.</returns>
        public static QueryNode Key(QuerySource source, object?[] keyValues)
        {
            EntityMetadata metadata = source.Metadata;
            QueryNode? condition = null;
            for (int i = 0; i < metadata.KeyColumns.Count; i++)
            {
                ColumnMetadata column = metadata.KeyColumns[i];
                QueryNode part = Equal(source, column, keyValues[i]);
                condition = condition == null ? part : new LogicalNode(LogicalOperator.And, condition, part);
            }

            return condition ?? new ValueNode(true, null, typeof(bool));
        }

        /// <summary>
        /// Builds <c>column = value</c>, or an IS NULL check for a null value.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="value">Value; may be null.</param>
        /// <returns>The condition.</returns>
        public static QueryNode Equal(QuerySource source, ColumnMetadata column, object? value)
        {
            ColumnNode columnNode = new ColumnNode(source, column);
            if (value == null) return new NullCheckNode(columnNode, true);
            return new ComparisonNode(ComparisonOperator.Equal, columnNode, new ValueNode(value, column, column.PropertyType), StringMatchMode.Database);
        }

        /// <summary>
        /// Builds the condition selecting rows that are not soft-deleted, or null when the entity has no soft-delete column.
        /// Boolean markers match false or null; date markers match null.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <returns>The condition or null.</returns>
        public static QueryNode? NotSoftDeleted(QuerySource source)
        {
            ColumnMetadata? column = source.Metadata.SoftDeleteColumn;
            if (column == null) return null;
            ColumnNode columnNode = new ColumnNode(source, column);
            NullCheckNode isNull = new NullCheckNode(columnNode, true);
            if (column.ClrType != typeof(bool)) return isNull;
            ComparisonNode isFalse = new ComparisonNode(ComparisonOperator.Equal, columnNode, new ValueNode(false, column, column.PropertyType), StringMatchMode.Database);
            return new LogicalNode(LogicalOperator.Or, isFalse, isNull);
        }

        /// <summary>
        /// Returns the value written to a soft-delete column when a row is deleted.
        /// </summary>
        /// <param name="column">Soft-delete column. Must not be null.</param>
        /// <returns>True for bool columns, otherwise the current UTC time.</returns>
        public static object SoftDeleteValue(ColumnMetadata column)
        {
            if (column.ClrType == typeof(bool)) return true;
            if (column.ClrType == typeof(DateTimeOffset)) return DateTimeOffset.UtcNow;
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Combines conditions with AND.
        /// </summary>
        /// <param name="conditions">Conditions; nulls are skipped. Must not be null.</param>
        /// <returns>The combined condition, or null when there is none.</returns>
        public static QueryNode? And(IEnumerable<QueryNode?> conditions)
        {
            QueryNode? combined = null;
            foreach (QueryNode? condition in conditions)
            {
                if (condition == null) continue;
                combined = combined == null ? condition : new LogicalNode(LogicalOperator.And, combined, condition);
            }

            return combined;
        }

        /// <summary>
        /// Builds an ascending or descending ordering on a column.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="descending">Whether the order is descending.</param>
        /// <returns>The ordering.</returns>
        public static QueryOrdering OrderBy(QuerySource source, ColumnMetadata column, bool descending)
        {
            ParameterExpression parameter = Expression.Parameter(source.Metadata.EntityType, "x");
            LambdaExpression selector = Expression.Lambda(Expression.Property(parameter, column.Property), parameter);
            return new QueryOrdering(new ColumnNode(source, column), selector, descending);
        }

        #endregion
    }
}
