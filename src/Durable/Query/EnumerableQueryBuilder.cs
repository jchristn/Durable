namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// A query builder evaluated on the client with LINQ-to-objects over an asynchronous source, used for projections
    /// (<see cref="IQueryBuilder{T}.Select{TResult}"/>) and grouped projections on backends that do not push them down.
    /// Where (combined with AND), ordering, Distinct (by mapped member values), Skip and Take are applied in that order to
    /// the source rows each time the query executes; lambdas are compiled null-safe, so reading a member of a null value
    /// yields the default instead of throwing (as a database yields NULL). Strings order ordinally and nulls sort first.
    /// Count, Any and the aggregates apply paging, like the SQL builders. Include, ThenInclude, GroupBy, Select and Delete
    /// throw <see cref="NotSupportedException"/>; IgnoreQueryFilters is forwarded to the source query when supported.
    /// Thread safety: not thread-safe; build and execute on one flow.
    /// </summary>
    /// <typeparam name="T">Result type.</typeparam>
    public class EnumerableQueryBuilder<T> : IQueryBuilder<T> where T : class, new()
    {
        #region Public-Members

        /// <inheritdoc />
        public string Query => _Describe != null ? _Describe() + Pipeline() : "client-side query over " + typeof(T).Name + Pipeline();

        /// <summary>
        /// Gets the capabilities checked by <see cref="Distinct"/> and the aggregates.
        /// </summary>
        public RepositoryCapabilities Capabilities { get; }

        #endregion

        #region Private-Members

        private readonly Func<CancellationToken, IAsyncEnumerable<T>> _Source;
        private readonly Func<string>? _Describe;
        private readonly Action? _IgnoreQueryFilters;
        private readonly List<Expression<Func<T, bool>>> _Predicates = new List<Expression<Func<T, bool>>>();
        private readonly List<KeyValuePair<LambdaExpression, bool>> _Orderings = new List<KeyValuePair<LambdaExpression, bool>>();
        private int? _Skip;
        private int? _Take;
        private bool _Distinct;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a builder.
        /// </summary>
        /// <param name="source">Source rows, produced each time the query executes. Must not be null.</param>
        /// <param name="capabilities">Capabilities checked by <see cref="Distinct"/> and the aggregates.</param>
        /// <param name="describe">Produces the description of the source for <see cref="Query"/>; may be null.</param>
        /// <param name="ignoreQueryFilters">Called by <see cref="IgnoreQueryFilters"/>; null makes it throw <see cref="NotSupportedException"/>.</param>
        /// <exception cref="ArgumentNullException">Thrown when source is null.</exception>
        public EnumerableQueryBuilder(Func<CancellationToken, IAsyncEnumerable<T>> source, RepositoryCapabilities capabilities, Func<string>? describe = null, Action? ignoreQueryFilters = null)
        {
            _Source = source ?? throw new ArgumentNullException(nameof(source));
            Capabilities = capabilities;
            _Describe = describe;
            _IgnoreQueryFilters = ignoreQueryFilters;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IQueryBuilder<T> Where(Expression<Func<T, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            _Predicates.Add(predicate);
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> Skip(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Skip count cannot be negative.");
            _Skip = count;
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> Take(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Take count cannot be negative.");
            _Take = count;
            return this;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Distinct"/>.</exception>
        public IQueryBuilder<T> Distinct()
        {
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Distinct, "Distinct");
            _Distinct = true;
            return this;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the source query cannot ignore filters.</exception>
        public IQueryBuilder<T> IgnoreQueryFilters()
        {
            if (_IgnoreQueryFilters == null) throw NotSupported("IgnoreQueryFilters");
            _IgnoreQueryFilters();
            return this;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new()
        {
            throw NotSupported("Select");
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public IQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty)
        {
            throw NotSupported("Include");
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public IQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            throw NotSupported("ThenInclude");
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public IGroupedQueryBuilder<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            throw NotSupported("GroupBy");
        }

        /// <inheritdoc />
        public IEnumerable<T> Execute()
        {
            return SyncBridge.Run(() => ExecuteListAsync(CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default)
        {
            return await ExecuteListAsync(token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<T> ExecuteAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            List<T> results = await ExecuteListAsync(token).ConfigureAwait(false);
            foreach (T item in results)
            {
                token.ThrowIfCancellationRequested();
                yield return item;
            }
        }

        /// <inheritdoc />
        public IDurableResult<T> ExecuteWithQuery()
        {
            string query = Query;
            return new DurableResult<T>(query, Execute());
        }

        /// <inheritdoc />
        public async Task<IDurableResult<T>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string query = Query;
            return new DurableResult<T>(query, await ExecuteListAsync(token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public IAsyncDurableResult<T> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            return new AsyncDurableResult<T>(Query, ExecuteAsyncEnumerable(token));
        }

        /// <inheritdoc />
        public long Count()
        {
            return SyncBridge.Run(() => CountAsync(CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(CancellationToken token = default)
        {
            return (await ExecuteListAsync(token).ConfigureAwait(false)).Count;
        }

        /// <inheritdoc />
        public bool Any()
        {
            return Count() > 0;
        }

        /// <inheritdoc />
        public async Task<bool> AnyAsync(CancellationToken token = default)
        {
            return await CountAsync(token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return SyncBridge.Run(() => SumAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            object? value = QueryAggregates.Sum(await ValuesAsync(selector, token).ConfigureAwait(false));
            return value == null ? 0m : ScalarConversion.To<decimal>(value);
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return SyncBridge.Run(() => AverageAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return QueryAggregates.Average(await ValuesAsync(selector, token).ConfigureAwait(false)) ?? 0m;
        }

        /// <inheritdoc />
        public TProperty Min<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return SyncBridge.Run(() => MinAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ScalarConversion.To<TProperty>(QueryAggregates.Min(await ValuesAsync(selector, token).ConfigureAwait(false)));
        }

        /// <inheritdoc />
        public TProperty Max<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return SyncBridge.Run(() => MaxAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ScalarConversion.To<TProperty>(QueryAggregates.Max(await ValuesAsync(selector, token).ConfigureAwait(false)));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public int Delete()
        {
            throw NotSupported("Delete");
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public Task<int> DeleteAsync(CancellationToken token = default)
        {
            throw NotSupported("Delete");
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Query;
        }

        #endregion

        #region Private-Methods

        private async Task<List<T>> ExecuteListAsync(CancellationToken token)
        {
            List<T> rows = new List<T>();
            await foreach (T row in _Source(token).WithCancellation(token).ConfigureAwait(false)) rows.Add(row);

            IEnumerable<T> query = rows;
            foreach (Expression<Func<T, bool>> predicate in _Predicates)
            {
                Func<T, bool> compiled = NullSafeExpressionRewriter.Compile(predicate);
                query = query.Where(compiled);
            }

            if (_Distinct) query = query.Distinct(new MappedValueEqualityComparer<T>());

            IOrderedEnumerable<T>? ordered = null;
            foreach (KeyValuePair<LambdaExpression, bool> ordering in _Orderings)
            {
                Func<T, object?> key = NullSafeExpressionRewriter.CompileBoxed<T>(ordering.Key);
                if (ordered == null)
                    ordered = ordering.Value ? query.OrderByDescending(key, QueryValueComparer.Ordinal) : query.OrderBy(key, QueryValueComparer.Ordinal);
                else
                    ordered = ordering.Value ? ordered.ThenByDescending(key, QueryValueComparer.Ordinal) : ordered.ThenBy(key, QueryValueComparer.Ordinal);
            }

            if (ordered != null) query = ordered;
            if (_Skip.HasValue) query = query.Skip(_Skip.Value);
            if (_Take.HasValue) query = query.Take(_Take.Value);
            return query.ToList();
        }

        private async Task<List<object?>> ValuesAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(selector);
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Aggregates, "Aggregate");
            Func<T, TProperty> compiled = NullSafeExpressionRewriter.Compile(selector);
            List<T> rows = await ExecuteListAsync(token).ConfigureAwait(false);
            return rows.Select(row => (object?)compiled(row)).ToList();
        }

        private string Pipeline()
        {
            List<string> parts = new List<string>();
            if (_Predicates.Count > 0) parts.Add(" WHERE " + string.Join(" AND ", _Predicates.Select(p => p.Body.ToString())));
            if (_Distinct) parts.Add(" DISTINCT");
            if (_Orderings.Count > 0) parts.Add(" ORDER BY " + string.Join(", ", _Orderings.Select(o => o.Key.Body + (o.Value ? " DESC" : " ASC"))));
            if (_Skip.HasValue) parts.Add(" SKIP " + _Skip.Value);
            if (_Take.HasValue) parts.Add(" TAKE " + _Take.Value);
            return string.Concat(parts);
        }

        private static NotSupportedException NotSupported(string operation)
        {
            return new NotSupportedException(operation + " is not supported on a projected query; apply it before Select.");
        }

        #endregion
    }
}
