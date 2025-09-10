namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Provides methods for building and executing grouped queries with aggregate operations.
    /// </summary>
    /// <typeparam name="T">The entity type being queried.</typeparam>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    public interface IGroupedQueryBuilder<T, TKey> where T : class, new()
    {
        /// <summary>
        /// Adds a HAVING clause to filter grouped results.
        /// </summary>
        /// <param name="predicate">The condition to apply to the grouped results.</param>
        /// <returns>The current grouped query builder for method chaining.</returns>
        IGroupedQueryBuilder<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate);
        
        /// <summary>
        /// Specifies a custom selection for the grouped query results.
        /// </summary>
        /// <typeparam name="TResult">The type of the result after selection.</typeparam>
        /// <param name="selector">The selection expression to apply to each group.</param>
        /// <returns>The current grouped query builder for method chaining.</returns>
        IGroupedQueryBuilder<T, TKey> Select<TResult>(Expression<Func<IGrouping<TKey, T>, TResult>> selector);
        
        /// <summary>
        /// Executes the grouped query and returns the results.
        /// </summary>
        /// <returns>The grouped query results.</returns>
        IEnumerable<IGrouping<TKey, T>> Execute();
        
        /// <summary>
        /// Asynchronously executes the grouped query and returns the results.
        /// </summary>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with grouped query results.</returns>
        Task<IEnumerable<IGrouping<TKey, T>>> ExecuteAsync(CancellationToken token = default);
        
        /// <summary>
        /// Returns the count of items in each group, optionally filtered by a predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter items before counting.</param>
        /// <returns>The count of items.</returns>
        int Count(Expression<Func<T, bool>>? predicate = null);
        
        /// <summary>
        /// Asynchronously returns the count of items in each group, optionally filtered by a predicate.
        /// </summary>
        /// <param name="predicate">Optional predicate to filter items before counting.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the count of items.</returns>
        Task<int> CountAsync(Expression<Func<T, bool>>? predicate = null, CancellationToken token = default);
        
        /// <summary>
        /// Calculates the sum of a numeric property for items in each group.
        /// </summary>
        /// <param name="selector">The property selector for the sum calculation.</param>
        /// <returns>The sum of the selected property.</returns>
        decimal Sum(Expression<Func<T, decimal>> selector);
        
        /// <summary>
        /// Asynchronously calculates the sum of a numeric property for items in each group.
        /// </summary>
        /// <param name="selector">The property selector for the sum calculation.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the sum of the selected property.</returns>
        Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, CancellationToken token = default);
        
        /// <summary>
        /// Calculates the average of a numeric property for items in each group.
        /// </summary>
        /// <param name="selector">The property selector for the average calculation.</param>
        /// <returns>The average of the selected property.</returns>
        decimal Average(Expression<Func<T, decimal>> selector);
        
        /// <summary>
        /// Asynchronously calculates the average of a numeric property for items in each group.
        /// </summary>
        /// <param name="selector">The property selector for the average calculation.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the average of the selected property.</returns>
        Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, CancellationToken token = default);
        
        /// <summary>
        /// Finds the maximum value of a property for items in each group.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared.</typeparam>
        /// <param name="selector">The property selector for finding the maximum.</param>
        /// <returns>The maximum value of the selected property.</returns>
        TResult Max<TResult>(Expression<Func<T, TResult>> selector);
        
        /// <summary>
        /// Asynchronously finds the maximum value of a property for items in each group.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared.</typeparam>
        /// <param name="selector">The property selector for finding the maximum.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the maximum value of the selected property.</returns>
        Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default);
        
        /// <summary>
        /// Finds the minimum value of a property for items in each group.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared.</typeparam>
        /// <param name="selector">The property selector for finding the minimum.</param>
        /// <returns>The minimum value of the selected property.</returns>
        TResult Min<TResult>(Expression<Func<T, TResult>> selector);
        
        /// <summary>
        /// Asynchronously finds the minimum value of a property for items in each group.
        /// </summary>
        /// <typeparam name="TResult">The type of the property being compared.</typeparam>
        /// <param name="selector">The property selector for finding the minimum.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the minimum value of the selected property.</returns>
        Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default);
    }
}