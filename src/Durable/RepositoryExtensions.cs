namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    public static class RepositoryExtensions
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
#pragma warning disable CS8604 // Possible null reference argument.
        
        public static IDurableResult<T> SelectWithQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return query.ExecuteWithQuery();
        }

        public static async Task<IDurableResult<T>> SelectWithQueryAsync<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return await query.ExecuteWithQueryAsync(token);
        }

        public static IAsyncDurableResult<T> SelectAsyncWithQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            IQueryBuilder<T> query = repository.Query(transaction);
            if (predicate != null)
                query = query.Where(predicate);

            return query.ExecuteAsyncEnumerableWithQuery(token);
        }

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