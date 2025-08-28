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

    public interface IGroupedQueryBuilder<T, TKey> where T : class, new()
    {
        IGroupedQueryBuilder<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate);
        IGroupedQueryBuilder<T, TKey> Select<TResult>(Expression<Func<IGrouping<TKey, T>, TResult>> selector);
        IEnumerable<IGrouping<TKey, T>> Execute();
        Task<IEnumerable<IGrouping<TKey, T>>> ExecuteAsync(CancellationToken token = default);
        
        // Aggregate methods
        int Count(Expression<Func<T, bool>> predicate = null);
        Task<int> CountAsync(Expression<Func<T, bool>> predicate = null, CancellationToken token = default);
        decimal Sum(Expression<Func<T, decimal>> selector);
        Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, CancellationToken token = default);
        decimal Average(Expression<Func<T, decimal>> selector);
        Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, CancellationToken token = default);
        TResult Max<TResult>(Expression<Func<T, TResult>> selector);
        Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default);
        TResult Min<TResult>(Expression<Func<T, TResult>> selector);
        Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default);
    }
}