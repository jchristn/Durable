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
    /// A grouped query evaluated on the client with LINQ-to-objects over rows supplied by a delegate, with the same
    /// observable results as the SQL grouped builder: <see cref="Execute"/> groups the source query's rows (its ordering
    /// and paging apply); <see cref="Count()"/> counts groups after Having; Sum, Average, Min and Max aggregate every row
    /// of the selected groups (zero or default when there are none); <see cref="Select{TResult}"/> projects each group
    /// (source ordering and paging do not apply, as with SQL GROUP BY). Lambdas are compiled null-safe.
    /// Thread safety: not thread-safe; build and execute on one flow.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <typeparam name="TKey">Group key type.</typeparam>
    public class EnumerableGroupedQueryBuilder<T, TKey> : IGroupedQueryBuilder<T, TKey> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the capabilities checked by the aggregates.
        /// </summary>
        public RepositoryCapabilities Capabilities { get; }

        #endregion

        #region Private-Members

        private readonly Func<bool, IReadOnlyList<LambdaExpression>, CancellationToken, Task<IReadOnlyList<T>>> _Rows;
        private readonly Expression<Func<T, TKey>> _KeySelector;
        private readonly List<Expression<Func<IGrouping<TKey, T>, bool>>> _Having = new List<Expression<Func<IGrouping<TKey, T>, bool>>>();
        private readonly Func<string>? _Describe;
        private readonly Action<LambdaExpression>? _Inspect;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a grouped builder.
        /// </summary>
        /// <param name="rows">
        /// Supplies the rows to group. Its arguments are whether the source query's ordering and paging apply, and the
        /// lambdas that will run over the rows (so the supplier can load the navigations they read). Must not be null.
        /// </param>
        /// <param name="keySelector">Group key selector. Must not be null.</param>
        /// <param name="capabilities">Capabilities checked by the aggregates.</param>
        /// <param name="describe">Produces the description of the source for query text; may be null.</param>
        /// <param name="inspect">Called with each lambda added to the query (key selector, Having, Select) so the caller can validate it; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when rows or keySelector is null.</exception>
        public EnumerableGroupedQueryBuilder(
            Func<bool, IReadOnlyList<LambdaExpression>, CancellationToken, Task<IReadOnlyList<T>>> rows,
            Expression<Func<T, TKey>> keySelector,
            RepositoryCapabilities capabilities,
            Func<string>? describe = null,
            Action<LambdaExpression>? inspect = null)
        {
            _Rows = rows ?? throw new ArgumentNullException(nameof(rows));
            _KeySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            Capabilities = capabilities;
            _Describe = describe;
            _Inspect = inspect;
            _Inspect?.Invoke(keySelector);
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IGroupedQueryBuilder<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            _Inspect?.Invoke(predicate);
            _Having.Add(predicate);
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<IGrouping<TKey, T>, TResult>> selector) where TResult : class, new()
        {
            ArgumentNullException.ThrowIfNull(selector);
            bool hasBindings;
            if (selector.Body is MemberInitExpression memberInit) hasBindings = memberInit.Bindings.Count > 0;
            else if (selector.Body is NewExpression newExpression && newExpression.Arguments.Count == 0) hasBindings = false;
            else throw new NotSupportedException("Select must use an object initializer, for example x => new Summary { Name = x.Name }.");
            _Inspect?.Invoke(selector);

            Func<IGrouping<TKey, T>, TResult> project = NullSafeExpressionRewriter.Compile(selector);
            List<LambdaExpression> lambdas = Lambdas(selector);
            return new EnumerableQueryBuilder<TResult>(
                token => ProjectAsync(project, lambdas, hasBindings, token),
                Capabilities,
                () => Describe() + " SELECT " + selector.Body);
        }

        /// <inheritdoc />
        public IEnumerable<IGrouping<TKey, T>> Execute()
        {
            return SyncBridge.Run(() => GroupsAsync(true, Lambdas(null), CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IGrouping<TKey, T>>> ExecuteAsync(CancellationToken token = default)
        {
            return await GroupsAsync(true, Lambdas(null), token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public long Count()
        {
            return SyncBridge.Run(() => CountAsync(CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(CancellationToken token = default)
        {
            return (await GroupsAsync(false, Lambdas(null), token).ConfigureAwait(false)).Count;
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
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector)
        {
            return SyncBridge.Run(() => MaxAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default)
        {
            return ScalarConversion.To<TResult>(QueryAggregates.Max(await ValuesAsync(selector, token).ConfigureAwait(false)));
        }

        /// <inheritdoc />
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector)
        {
            return SyncBridge.Run(() => MinAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default)
        {
            return ScalarConversion.To<TResult>(QueryAggregates.Min(await ValuesAsync(selector, token).ConfigureAwait(false)));
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Describe();
        }

        #endregion

        #region Private-Methods

        private string Describe()
        {
            string source = _Describe != null ? _Describe() : "client-side query over " + typeof(T).Name;
            string having = _Having.Count == 0 ? string.Empty : " HAVING " + string.Join(" AND ", _Having.Select(h => h.Body.ToString()));
            return source + " GROUP BY " + _KeySelector.Body + having;
        }

        private List<LambdaExpression> Lambdas(LambdaExpression? extra)
        {
            List<LambdaExpression> lambdas = new List<LambdaExpression> { _KeySelector };
            lambdas.AddRange(_Having);
            if (extra != null) lambdas.Add(extra);
            return lambdas;
        }

        private async Task<List<IGrouping<TKey, T>>> GroupsAsync(bool orderingAndPaging, IReadOnlyList<LambdaExpression> lambdas, CancellationToken token)
        {
            IReadOnlyList<T> rows = await _Rows(orderingAndPaging, lambdas, token).ConfigureAwait(false);
            Func<T, TKey> key = NullSafeExpressionRewriter.Compile(_KeySelector);
            IEnumerable<IGrouping<TKey, T>> groups = rows.GroupBy(key);
            foreach (Expression<Func<IGrouping<TKey, T>, bool>> having in _Having)
            {
                Func<IGrouping<TKey, T>, bool> predicate = NullSafeExpressionRewriter.Compile(having);
                groups = groups.Where(predicate);
            }

            return groups.ToList();
        }

        private async IAsyncEnumerable<TResult> ProjectAsync<TResult>(Func<IGrouping<TKey, T>, TResult> project, List<LambdaExpression> lambdas, bool hasBindings, [EnumeratorCancellation] CancellationToken token)
        {
            if (!hasBindings) throw new NotSupportedException("Select must assign at least one member.");
            List<IGrouping<TKey, T>> groups = await GroupsAsync(false, lambdas, token).ConfigureAwait(false);
            foreach (IGrouping<TKey, T> group in groups)
            {
                token.ThrowIfCancellationRequested();
                yield return project(group);
            }
        }

        private async Task<List<object?>> ValuesAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(selector);
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Aggregates, "Aggregate");
            _Inspect?.Invoke(selector);
            Func<T, TProperty> compiled = NullSafeExpressionRewriter.Compile(selector);
            List<IGrouping<TKey, T>> groups = await GroupsAsync(false, Lambdas(selector), token).ConfigureAwait(false);
            return groups.SelectMany(g => g).Select(row => (object?)compiled(row)).ToList();
        }

        #endregion
    }
}
