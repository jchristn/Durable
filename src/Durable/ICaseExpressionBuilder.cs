namespace Durable
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a fluent interface for building SQL CASE expressions in queries.
    /// </summary>
    /// <typeparam name="T">The entity type being queried.</typeparam>
    public interface ICaseExpressionBuilder<T> where T : class, new()
    {
        /// <summary>
        /// Adds a WHEN condition to the CASE expression using a strongly-typed expression.
        /// </summary>
        /// <param name="condition">The condition expression to evaluate.</param>
        /// <param name="result">The value to return when the condition is true.</param>
        /// <returns>The case expression builder for method chaining.</returns>
        ICaseExpressionBuilder<T> When(Expression<Func<T, bool>> condition, object result);

        /// <summary>
        /// Adds a WHEN condition to the CASE expression using raw SQL.
        /// </summary>
        /// <param name="condition">The raw SQL condition string.</param>
        /// <param name="result">The value to return when the condition is true.</param>
        /// <returns>The case expression builder for method chaining.</returns>
        ICaseExpressionBuilder<T> WhenRaw(string condition, object result);

        /// <summary>
        /// Adds an ELSE clause to the CASE expression for the default value.
        /// </summary>
        /// <param name="result">The default value to return when no conditions match.</param>
        /// <returns>The case expression builder for method chaining.</returns>
        ICaseExpressionBuilder<T> Else(object result);

        /// <summary>
        /// Completes the CASE expression and returns it as a query builder column with the specified alias.
        /// </summary>
        /// <param name="alias">The column alias for the CASE expression result.</param>
        /// <returns>The query builder for continued query construction.</returns>
        IQueryBuilder<T> EndCase(string alias);
    }
}