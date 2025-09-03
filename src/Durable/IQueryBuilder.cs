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
        IQueryBuilder<T> Having(Expression<Func<T, bool>> predicate);

        // Set operations
        IQueryBuilder<T> Union(IQueryBuilder<T> other);
        IQueryBuilder<T> UnionAll(IQueryBuilder<T> other);
        IQueryBuilder<T> Intersect(IQueryBuilder<T> other);
        IQueryBuilder<T> Except(IQueryBuilder<T> other);

        // Subquery support
        IQueryBuilder<T> WhereIn<TKey>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new();
        IQueryBuilder<T> WhereNotIn<TKey>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TKey> subquery) where TKey : class, new();
        IQueryBuilder<T> WhereInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql);
        IQueryBuilder<T> WhereNotInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql);
        IQueryBuilder<T> WhereExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new();
        IQueryBuilder<T> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery) where TOther : class, new();

        // Window functions
        IWindowedQueryBuilder<T> WithWindowFunction(string functionName, string partitionBy = null, string orderBy = null);

        // CTEs (Common Table Expressions)
        IQueryBuilder<T> WithCte(string cteName, string cteQuery);
        IQueryBuilder<T> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery);

        // Custom SQL fragments
        IQueryBuilder<T> WhereRaw(string sql, params object[] parameters);
        IQueryBuilder<T> SelectRaw(string sql);
        IQueryBuilder<T> FromRaw(string sql);
        IQueryBuilder<T> JoinRaw(string sql);

        // CASE WHEN expressions
        ICaseExpressionBuilder<T> SelectCase();

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