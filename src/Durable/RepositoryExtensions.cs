namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides extension methods for IRepository to simplify query operations.
    /// </summary>
    public static class RepositoryExtensions
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning disable CS8604 // Possible null reference argument.
        
        /// <summary>
        /// Executes a query with an optional predicate and returns the results with the generated SQL query.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository to query.</param>
        /// <param name="predicate">Optional predicate to filter results.</param>
        /// <param name="transaction">Optional transaction to use for the query.</param>
        /// <returns>A durable result containing the entities and the generated SQL query.</returns>
        public static IDurableResult<T> SelectWithQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return query.ExecuteWithQuery();
        }

        /// <summary>
        /// Asynchronously executes a query with an optional predicate and returns the results with the generated SQL query.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository to query.</param>
        /// <param name="predicate">Optional predicate to filter results.</param>
        /// <param name="transaction">Optional transaction to use for the query.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that resolves to a durable result containing the entities and the generated SQL query.</returns>
        public static async Task<IDurableResult<T>> SelectWithQueryAsync<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return await query.ExecuteWithQueryAsync(token);
        }

        /// <summary>
        /// Returns an async enumerable query result with an optional predicate and the generated SQL query.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository to query.</param>
        /// <param name="predicate">Optional predicate to filter results.</param>
        /// <param name="transaction">Optional transaction to use for the query.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>An async durable result that can be enumerated asynchronously.</returns>
        public static IAsyncDurableResult<T> SelectAsyncWithQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return query.ExecuteAsyncEnumerableWithQuery(token);
        }

        /// <summary>
        /// Gets the generated SQL query string for a select operation with an optional predicate.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository to query.</param>
        /// <param name="predicate">Optional predicate to filter results.</param>
        /// <param name="transaction">Optional transaction to use for the query.</param>
        /// <returns>The generated SQL query string.</returns>
        public static string GetSelectQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return query.Query;
        }
        
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning restore CS8604 // Possible null reference argument.
    }
}