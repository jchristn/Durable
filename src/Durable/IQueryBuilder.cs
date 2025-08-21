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

    public interface IQueryBuilder<T> where T : class, new()
    {
        IQueryBuilder<T> Where(Expression<Func<T, bool>> predicate);
        IQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector);
        IQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);
        IQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector);
        IQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector);
        IQueryBuilder<T> Skip(int count);
        IQueryBuilder<T> Take(int count);
        IQueryBuilder<T> Distinct();

        // Projection support
        IQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new();

        // Join/Include support
        IQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty);
        IQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty);

        // Group by support
        IGroupedQueryBuilder<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector);

        // Execution
        IEnumerable<T> Execute();
        Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default);
        IAsyncEnumerable<T> ExecuteAsyncEnumerable(CancellationToken token = default);

        // Execution with query exposure
        IDurableResult<T> ExecuteWithQuery();
        Task<IDurableResult<T>> ExecuteWithQueryAsync(CancellationToken token = default);
        IAsyncDurableResult<T> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default);

        // Query property for direct access
        string Query { get; }

        // For debugging (existing)
        string BuildSql();
    }
}