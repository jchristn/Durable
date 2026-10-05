namespace Durable.Sql
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Builds a CASE expression appended to a query's select list. Result values are bound as parameters.
    /// Thread safety: not thread-safe.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public interface ICaseExpressionBuilder<T> where T : class, new()
    {
        /// <summary>
        /// Adds a WHEN branch.
        /// </summary>
        /// <param name="condition">Condition. Must not be null.</param>
        /// <param name="result">Result value; may be null.</param>
        /// <returns>This builder.</returns>
        ICaseExpressionBuilder<T> When(Expression<Func<T, bool>> condition, object? result);

        /// <summary>
        /// Adds a WHEN branch with a raw SQL condition.
        /// </summary>
        /// <param name="condition">Condition SQL. Must not be null.</param>
        /// <param name="result">Result value; may be null.</param>
        /// <returns>This builder.</returns>
        ICaseExpressionBuilder<T> WhenRaw(string condition, object? result);

        /// <summary>
        /// Sets the ELSE result.
        /// </summary>
        /// <param name="result">Result value; may be null.</param>
        /// <returns>This builder.</returns>
        ICaseExpressionBuilder<T> Else(object? result);

        /// <summary>
        /// Completes the expression and returns to the query.
        /// </summary>
        /// <param name="alias">Column alias. Must not be null; letters, digits and underscores.</param>
        /// <returns>The query builder.</returns>
        /// <exception cref="ArgumentException">Thrown when alias is invalid or no WHEN branch was added.</exception>
        ISqlQueryBuilder<T> EndCase(string alias);
    }
}
