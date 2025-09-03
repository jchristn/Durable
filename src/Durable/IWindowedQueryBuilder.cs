namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IWindowedQueryBuilder<T> where T : class, new()
    {
        // Window function configuration
        IWindowedQueryBuilder<T> RowNumber(string alias = "row_number");
        IWindowedQueryBuilder<T> Rank(string alias = "rank");
        IWindowedQueryBuilder<T> DenseRank(string alias = "dense_rank");
        IWindowedQueryBuilder<T> Lead<TKey>(Expression<Func<T, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lead");
        IWindowedQueryBuilder<T> Lag<TKey>(Expression<Func<T, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lag");
        IWindowedQueryBuilder<T> FirstValue<TKey>(Expression<Func<T, TKey>> column, string alias = "first_value");
        IWindowedQueryBuilder<T> LastValue<TKey>(Expression<Func<T, TKey>> column, string alias = "last_value");
        IWindowedQueryBuilder<T> NthValue<TKey>(Expression<Func<T, TKey>> column, int n, string alias = "nth_value");
        IWindowedQueryBuilder<T> Sum<TKey>(Expression<Func<T, TKey>> column, string alias = "sum");
        IWindowedQueryBuilder<T> Avg<TKey>(Expression<Func<T, TKey>> column, string alias = "avg");
        IWindowedQueryBuilder<T> Count(string alias = "count");
        IWindowedQueryBuilder<T> Min<TKey>(Expression<Func<T, TKey>> column, string alias = "min");
        IWindowedQueryBuilder<T> Max<TKey>(Expression<Func<T, TKey>> column, string alias = "max");

        // Window partitioning and ordering
        IWindowedQueryBuilder<T> PartitionBy<TKey>(Expression<Func<T, TKey>> keySelector);
        IWindowedQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector);
        IWindowedQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

        // Window frame specification
        IWindowedQueryBuilder<T> Rows(int preceding, int following);
        IWindowedQueryBuilder<T> RowsUnboundedPreceding();
        IWindowedQueryBuilder<T> RowsUnboundedFollowing();
        IWindowedQueryBuilder<T> RowsBetween(string start, string end);
        IWindowedQueryBuilder<T> Range(int preceding, int following);
        IWindowedQueryBuilder<T> RangeUnboundedPreceding();
        IWindowedQueryBuilder<T> RangeUnboundedFollowing();
        IWindowedQueryBuilder<T> RangeBetween(string start, string end);

        // Return to regular query builder
        IQueryBuilder<T> EndWindow();

        // Execution methods (inherit from base query)
        IEnumerable<T> Execute();
        Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default);
        IAsyncEnumerable<T> ExecuteAsyncEnumerable(CancellationToken token = default);
    }
}