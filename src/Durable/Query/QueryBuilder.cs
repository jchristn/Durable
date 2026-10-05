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
    /// The backend-neutral <see cref="IQueryBuilder{T}"/> used by <see cref="RepositoryBase{T}"/>. Builder calls are
    /// recorded and normalized into a <see cref="QueryModel"/> only when the query executes, so captured variables are read
    /// at execution time (as with SQL). The model combines the soft-delete condition, the repository's query filters
    /// (unless <see cref="IgnoreQueryFilters"/> is called) and the Where predicates with AND, and is executed through the
    /// repository's <see cref="IRepositoryBackend"/>. Includes are loaded afterwards with split queries by a
    /// <see cref="BackendIncludeLoader"/>. Select and GroupBy are evaluated on the client over the filtered, ordered rows
    /// (navigations the lambdas read are loaded automatically); override them to push projections or grouping down.
    /// Count, Any and the aggregates apply Skip/Take when set, like the SQL builder.
    /// Capability checks happen when a builder method is called: Include needs <see cref="RepositoryCapabilities.Include"/>
    /// (and <see cref="RepositoryCapabilities.ManyToMany"/> for many-to-many navigations), Distinct needs
    /// <see cref="RepositoryCapabilities.Distinct"/>, Select needs <see cref="RepositoryCapabilities.Projection"/>, GroupBy
    /// needs <see cref="RepositoryCapabilities.Grouping"/> and Sum/Average/Min/Max need
    /// <see cref="RepositoryCapabilities.Aggregates"/>. Predicates and orderings are validated when the query executes.
    /// Thread safety: not thread-safe; build and execute on one flow.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class QueryBuilder<T> : IQueryBuilder<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the repository the query runs against. Never null.
        /// </summary>
        public RepositoryBase<T> Repository { get; }

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata => Repository.Metadata;

        /// <summary>
        /// Gets the explicit transaction; null to use the ambient transaction, if any.
        /// </summary>
        public ITransaction? Transaction { get; }

        /// <summary>
        /// Gets the include tree recorded by Include/ThenInclude. Never null.
        /// </summary>
        public IncludeTree Includes { get; }

        /// <inheritdoc />
        public string Query => Repository.DescribeQuery(BuildModel(true));

        #endregion

        #region Private-Members

        private readonly List<Func<QueryNormalizer, QuerySource, QueryNode>> _Conditions = new List<Func<QueryNormalizer, QuerySource, QueryNode>>();
        private readonly List<KeyValuePair<LambdaExpression, bool>> _Orderings = new List<KeyValuePair<LambdaExpression, bool>>();
        private int? _Skip;
        private int? _Take;
        private bool _Distinct;
        private bool _IgnoreQueryFilters;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a query builder.
        /// </summary>
        /// <param name="repository">Repository. Must not be null.</param>
        /// <param name="transaction">Explicit transaction; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public QueryBuilder(RepositoryBase<T> repository, ITransaction? transaction = null)
        {
            Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            Transaction = transaction;
            Includes = new IncludeTree(repository.Metadata);
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IQueryBuilder<T> Where(Expression<Func<T, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ValidateAtCallSite(predicate);
            _Conditions.Add((normalizer, source) => normalizer.NormalizeLambda(predicate, source));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ValidateAtCallSite(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ValidateAtCallSite(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ValidateAtCallSite(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ValidateAtCallSite(keySelector);
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
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Distinct, "Distinct");
            _Distinct = true;
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<T> IgnoreQueryFilters()
        {
            _IgnoreQueryFilters = true;
            return this;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Projection"/> or the selector is not an object initializer.</exception>
        public virtual IQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new()
        {
            ArgumentNullException.ThrowIfNull(selector);
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Projection, "Select");
            bool hasBindings;
            if (selector.Body is MemberInitExpression memberInit) hasBindings = memberInit.Bindings.Count > 0;
            else if (selector.Body is NewExpression newExpression && newExpression.Arguments.Count == 0) hasBindings = false;
            else throw new NotSupportedException("Select must use an object initializer, for example x => new Summary { Name = x.Name }.");

            List<LambdaExpression> navigations = NavigationPathCollector.Collect(typeof(T), new LambdaExpression[] { selector });
            RequireNavigationLoading(navigations);
            Func<T, TResult> project = NullSafeExpressionRewriter.Compile(selector);
            return new EnumerableQueryBuilder<TResult>(
                token => ProjectAsync(project, navigations, hasBindings, token),
                Repository.Capabilities,
                () => Query + " SELECT " + selector.Body,
                () => IgnoreQueryFilters());
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Include"/>, or <see cref="RepositoryCapabilities.ManyToMany"/> for a many-to-many navigation.</exception>
        public IQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty)
        {
            ArgumentNullException.ThrowIfNull(navigationProperty);
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Include, "Include");
            Includes.Include(navigationProperty);
            RequireManyToManyFor(Includes);
            return this;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Include"/>, or <see cref="RepositoryCapabilities.ManyToMany"/> for a many-to-many navigation.</exception>
        public IQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            ArgumentNullException.ThrowIfNull(navigationProperty);
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Include, "ThenInclude");
            Includes.ThenInclude(typeof(TPreviousProperty), navigationProperty);
            RequireManyToManyFor(Includes);
            return this;
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Grouping"/>.</exception>
        public virtual IGroupedQueryBuilder<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Grouping, "GroupBy");
            return new EnumerableGroupedQueryBuilder<T, TKey>(
                async (orderingAndPaging, lambdas, token) => await LoadRowsAsync(orderingAndPaging, NavigationPathCollector.Collect(typeof(T), lambdas), token).ConfigureAwait(false),
                keySelector,
                Repository.Capabilities,
                () => Repository.DescribeQuery(BuildModel(false)),
                lambda => RequireNavigationLoading(NavigationPathCollector.Collect(typeof(T), new[] { lambda })));
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
            QueryModel model = BuildModel(true);
            if (Includes.IsEmpty)
            {
                await foreach (T item in StreamAsync(model, token).ConfigureAwait(false)) yield return item;
                yield break;
            }

            if (model.Transaction != null)
            {
                foreach (T item in await ExecuteModelAsync(model, token).ConfigureAwait(false)) yield return item;
                yield break;
            }

            BackendIncludeLoader loader = Repository.CreateIncludeLoader();
            int batchSize = Repository.IncludeStreamingBatchSize;
            List<T> batch = new List<T>(batchSize);
            await foreach (T item in StreamAsync(model, token).ConfigureAwait(false))
            {
                batch.Add(item);
                if (batch.Count < batchSize) continue;
                await loader.LoadAsync(batch, Includes.Nodes, null, token).ConfigureAwait(false);
                foreach (T loaded in batch) yield return loaded;
                batch.Clear();
            }

            if (batch.Count > 0)
            {
                await loader.LoadAsync(batch, Includes.Nodes, null, token).ConfigureAwait(false);
                foreach (T loaded in batch) yield return loaded;
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
            IEnumerable<T> results = await ExecuteListAsync(token).ConfigureAwait(false);
            return new DurableResult<T>(query, results);
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
        public Task<long> CountAsync(CancellationToken token = default)
        {
            QueryModel model = BuildModel(true);
            return Repository.Backend.CountAsync(model, token);
        }

        /// <inheritdoc />
        public bool Any()
        {
            return SyncBridge.Run(() => AnyAsync(CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<bool> AnyAsync(CancellationToken token = default)
        {
            QueryModel model = BuildModel(true);
            model.Take = model.Take.HasValue ? Math.Min(model.Take.Value, 1) : 1;
            return await Repository.Backend.CountAsync(model, token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return SyncBridge.Run(() => SumAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            object? value = await AggregateAsync(AggregateFunction.Sum, selector, token).ConfigureAwait(false);
            return value == null ? 0m : ScalarConversion.To<decimal>(value);
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return SyncBridge.Run(() => AverageAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            object? value = await AggregateAsync(AggregateFunction.Average, selector, token).ConfigureAwait(false);
            return value == null ? 0m : ScalarConversion.To<decimal>(value);
        }

        /// <inheritdoc />
        public TProperty Min<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return SyncBridge.Run(() => MinAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ScalarConversion.To<TProperty>(await AggregateAsync(AggregateFunction.Min, selector, token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public TProperty Max<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return SyncBridge.Run(() => MaxAsync(selector, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ScalarConversion.To<TProperty>(await AggregateAsync(AggregateFunction.Max, selector, token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when combined with Skip, Take or Distinct.</exception>
        public int Delete()
        {
            return SyncBridge.Run(() => DeleteAsync(CancellationToken.None));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when combined with Skip, Take or Distinct.</exception>
        public Task<int> DeleteAsync(CancellationToken token = default)
        {
            if (_Skip.HasValue || _Take.HasValue || _Distinct)
                throw new NotSupportedException("Delete cannot be combined with Skip, Take, Distinct, set operations, or raw FROM/JOIN/SELECT fragments.");
            return Repository.DeleteModelAsync(BuildModel(false), token);
        }

        /// <summary>
        /// Normalizes the recorded state into a backend query model. Captured variables are read now.
        /// </summary>
        /// <param name="includeOrderingAndPaging">Whether ordering, Skip and Take are included.</param>
        /// <returns>The model, with the resolved transaction. Never null.</returns>
        /// <exception cref="NotSupportedException">Thrown when a predicate or ordering cannot be translated or uses an unsupported capability.</exception>
        public QueryModel BuildModel(bool includeOrderingAndPaging)
        {
            QuerySource source = new QuerySource(Metadata, Metadata.TableName);
            QueryNormalizer normalizer = Repository.CreateNormalizer();
            List<QueryNode?> conditions = new List<QueryNode?>();
            if (!_IgnoreQueryFilters)
            {
                conditions.Add(QueryConditions.NotSoftDeleted(source));
                foreach (Expression<Func<T, bool>> filter in Repository.QueryFilters) conditions.Add(normalizer.NormalizeLambda(filter, source));
            }

            foreach (Func<QueryNormalizer, QuerySource, QueryNode> condition in _Conditions) conditions.Add(condition(normalizer, source));

            QueryModel model = new QueryModel(source)
            {
                Filter = QueryConditions.And(conditions),
                Distinct = _Distinct,
                Transaction = Repository.ResolveTransaction(Transaction)
            };

            if (includeOrderingAndPaging)
            {
                foreach (KeyValuePair<LambdaExpression, bool> ordering in _Orderings)
                    model.Orderings.Add(new QueryOrdering(normalizer.NormalizeLambda(ordering.Key, source), ordering.Key, ordering.Value));
                model.Skip = _Skip;
                model.Take = _Take;
            }

            RepositoryCapabilities capabilities = Repository.Capabilities;
            QueryCapabilityValidator.Validate(model.Filter, capabilities);
            foreach (QueryOrdering ordering in model.Orderings) QueryCapabilityValidator.Validate(ordering.Key, capabilities);
            return model;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Query;
        }

        #endregion

        #region Private-Methods

        private void ValidateAtCallSite(LambdaExpression lambda)
        {
            // Normalize now only to reject unsupported features where the caller wrote them. The real normalization
            // happens at execution so captured variables are read then; errors other than NotSupportedException (for
            // example a captured object that is still null) are left to surface at execution, as before.
            QueryNode node;
            try
            {
                node = Repository.CreateNormalizer().NormalizeLambda(lambda, new QuerySource(Metadata));
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception)
            {
                return;
            }

            QueryCapabilityValidator.Validate(node, Repository.Capabilities);
        }

        /// <summary>
        /// Adds a primary-key condition.
        /// </summary>
        /// <param name="keyValues">One value per key column, in key order. Never null.</param>
        /// <returns>This builder.</returns>
        internal QueryBuilder<T> WhereKey(object?[] keyValues)
        {
            _Conditions.Add((normalizer, source) => QueryConditions.Key(source, keyValues));
            return this;
        }

        /// <summary>
        /// Streams the entities of a model from the backend (includes are not loaded).
        /// </summary>
        /// <param name="model">Model. Never null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The entities.</returns>
        internal async IAsyncEnumerable<T> StreamAsync(QueryModel model, [EnumeratorCancellation] CancellationToken token)
        {
            await foreach (object item in Repository.Backend.QueryAsync(model, token).ConfigureAwait(false)) yield return (T)item;
        }

        /// <summary>
        /// Reads the rows of this query (with or without its ordering and paging), loads its includes plus the extra
        /// navigation paths, and returns them.
        /// </summary>
        /// <param name="includeOrderingAndPaging">Whether ordering, Skip and Take apply.</param>
        /// <param name="navigationPaths">Additional navigation-access lambdas to load. Never null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The rows. Never null.</returns>
        protected async Task<IReadOnlyList<T>> LoadRowsAsync(bool includeOrderingAndPaging, IReadOnlyList<LambdaExpression> navigationPaths, CancellationToken token)
        {
            QueryModel model = BuildModel(includeOrderingAndPaging);
            List<T> rows = await ExecuteModelAsync(model, token).ConfigureAwait(false);
            if (navigationPaths.Count > 0 && rows.Count > 0)
            {
                IncludeTree tree = new IncludeTree(Metadata);
                foreach (LambdaExpression path in navigationPaths) tree.Include(path);
                await Repository.CreateIncludeLoader().LoadAsync(rows, tree.Nodes, model.Transaction, token).ConfigureAwait(false);
            }

            return rows;
        }

        private async Task<List<T>> ExecuteListAsync(CancellationToken token)
        {
            return await ExecuteModelAsync(BuildModel(true), token).ConfigureAwait(false);
        }

        private async Task<List<T>> ExecuteModelAsync(QueryModel model, CancellationToken token)
        {
            List<T> results = new List<T>();
            await foreach (T item in StreamAsync(model, token).ConfigureAwait(false)) results.Add(item);
            if (!Includes.IsEmpty && results.Count > 0)
                await Repository.CreateIncludeLoader().LoadAsync(results, Includes.Nodes, model.Transaction, token).ConfigureAwait(false);
            return results;
        }

        private async IAsyncEnumerable<TResult> ProjectAsync<TResult>(Func<T, TResult> project, List<LambdaExpression> navigations, bool hasBindings, [EnumeratorCancellation] CancellationToken token)
        {
            if (!hasBindings) throw new NotSupportedException("Select must assign at least one member.");
            IReadOnlyList<T> rows = await LoadRowsAsync(true, navigations, token).ConfigureAwait(false);
            foreach (T row in rows)
            {
                token.ThrowIfCancellationRequested();
                yield return project(row);
            }
        }

        private async Task<object?> AggregateAsync(AggregateFunction function, LambdaExpression selector, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(selector);
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Aggregates, function.ToString());
            QueryModel model = BuildModel(true);
            QueryNode operand = Repository.CreateNormalizer().NormalizeLambda(selector, model.Source);
            QueryCapabilityValidator.Validate(operand, Repository.Capabilities);
            return await Repository.Backend.AggregateAsync(model, function, operand, token).ConfigureAwait(false);
        }

        private void RequireNavigationLoading(List<LambdaExpression> navigations)
        {
            if (navigations.Count == 0) return;
            QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.Include, "Navigation access in a client-evaluated projection or grouping");
            IncludeTree tree = new IncludeTree(Metadata);
            foreach (LambdaExpression path in navigations) tree.Include(path);
            RequireManyToManyFor(tree);
        }

        private void RequireManyToManyFor(IncludeTree tree)
        {
            foreach (IncludeNode node in tree.All())
            {
                if (node.Navigation.Kind == NavigationKind.ManyToMany)
                    QueryCapabilityValidator.Require(Repository.Capabilities, RepositoryCapabilities.ManyToMany, "Include of many-to-many navigation '" + node.Navigation.Name + "'");
            }
        }

        #endregion
    }
}
