namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.ConcurrencyConflictResolvers;

    /// <summary>
    /// The complete <see cref="IRepository{T}"/> implemented over an <see cref="IRepositoryBackend"/>, so a non-SQL
    /// backend (document store, search engine, graph store, in-memory) only implements the small backend contract.
    /// Observable behavior matches the SQL repositories: return values, exception types and messages, key handling
    /// (a scalar id, or an <see cref="object"/> array for composite keys), generated-key write-back, optimistic
    /// concurrency with <see cref="ConflictResolver"/>, version bumps on set-based updates, soft delete, query filters,
    /// upsert and transactions (explicit, or the ambient <see cref="TransactionScope.Current"/>).
    /// <para>
    /// Capabilities: entities with composite keys require <see cref="RepositoryCapabilities.CompositeKeys"/> and entities
    /// with a version column require <see cref="RepositoryCapabilities.OptimisticConcurrency"/> (checked by the
    /// constructor); <see cref="UpdateField{TField}"/> and <see cref="BatchUpdate"/> require
    /// <see cref="RepositoryCapabilities.BatchUpdate"/>; <see cref="Upsert"/> requires <see cref="RepositoryCapabilities.Upsert"/>;
    /// Sum, Average, Min and Max require <see cref="RepositoryCapabilities.Aggregates"/>; <see cref="BeginTransaction"/>
    /// requires <see cref="RepositoryCapabilities.Transactions"/>. Every normalized filter, ordering, aggregate operand and
    /// assignment is validated with <see cref="QueryCapabilityValidator"/> before it reaches the backend. Operations that
    /// run "in a single transaction when none is supplied" (CreateMany, UpdateMany, Upsert, UpsertMany) start one only
    /// when the backend supports transactions; otherwise they run without atomicity.
    /// </para>
    /// <para>
    /// Synchronous members run the asynchronous backend calls through a bridge that never deadlocks under a
    /// synchronization context: the call is started inline with no synchronization context installed, so a backend that
    /// completes synchronously returns directly without blocking or hopping threads; otherwise its continuations run on
    /// the thread pool while the caller blocks.
    /// </para>
    /// Thread safety: safe for concurrent use once configured. Configuration members (<see cref="AddQueryFilter"/>,
    /// <see cref="ClearQueryFilters"/>, <see cref="ConflictResolver"/>, <see cref="IncludeStreamingBatchSize"/>,
    /// <see cref="IncludeChunkSize"/>) should be set before concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class RepositoryBase<T> : IRepository<T> where T : class, new()
    {
        #region Public-Members

        /// <inheritdoc />
        public EntityMetadata Metadata { get; }

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => Backend.Capabilities;

        /// <summary>
        /// Gets the backend. Never null. The repository does not own or dispose it.
        /// </summary>
        public IRepositoryBackend Backend { get; }

        /// <summary>
        /// Gets the options. Never null.
        /// </summary>
        public RepositoryOptions Options { get; }

        /// <inheritdoc />
        public IReadOnlyList<Expression<Func<T, bool>>> QueryFilters => _QueryFilters;

        /// <summary>
        /// Gets or sets the resolver consulted when an update fails its version check.
        /// Default: a <see cref="DefaultConflictResolver{T}"/> with <see cref="ConflictResolutionStrategy.ThrowException"/>.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when set to null.</exception>
        public IConcurrencyConflictResolver<T> ConflictResolver
        {
            get => _ConflictResolver;
            set => _ConflictResolver = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Gets or sets how many root entities are buffered before loading includes while streaming with
        /// <see cref="IQueryBuilder{T}.ExecuteAsyncEnumerable"/>. Default: 256. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set below 1.</exception>
        public int IncludeStreamingBatchSize
        {
            get => _IncludeStreamingBatchSize;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "IncludeStreamingBatchSize must be at least 1.");
                _IncludeStreamingBatchSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of keys in one related-row query issued by includes. Default: 500. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set below 1.</exception>
        public int IncludeChunkSize
        {
            get => _IncludeChunkSize;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "IncludeChunkSize must be at least 1.");
                _IncludeChunkSize = value;
            }
        }

        #endregion

        #region Private-Members

        private readonly IReadOnlyList<ColumnMetadata> _UpdateColumns;
        private volatile IReadOnlyList<Expression<Func<T, bool>>> _QueryFilters = Array.Empty<Expression<Func<T, bool>>>();
        private IConcurrencyConflictResolver<T> _ConflictResolver = new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
        private int _IncludeStreamingBatchSize = 256;
        private int _IncludeChunkSize = 500;
        private int _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a repository over a backend.
        /// </summary>
        /// <param name="backend">Backend. Must not be null. Not disposed by the repository.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity has a composite key or a version column the backend does not support.</exception>
        public RepositoryBase(IRepositoryBackend backend, RepositoryOptions? options = null)
        {
            Backend = backend ?? throw new ArgumentNullException(nameof(backend));
            Options = options ?? new RepositoryOptions();
            Metadata = EntityMetadata.For(typeof(T));
            Metadata.RequireKey();
            if (Metadata.HasCompositeKey)
                QueryCapabilityValidator.Require(Backend.Capabilities, RepositoryCapabilities.CompositeKeys, "Entity " + typeof(T).Name + " with a composite key");
            if (Metadata.VersionColumn != null)
                QueryCapabilityValidator.Require(Backend.Capabilities, RepositoryCapabilities.OptimisticConcurrency, "Entity " + typeof(T).Name + " with a version column");
            _UpdateColumns = Metadata.Columns.Where(c => !c.IsPrimaryKey && !c.IsAutoIncrement).ToList();
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public void AddQueryFilter(Expression<Func<T, bool>> filter)
        {
            ArgumentNullException.ThrowIfNull(filter);
            List<Expression<Func<T, bool>>> updated = new List<Expression<Func<T, bool>>>(_QueryFilters) { filter };
            _QueryFilters = updated;
        }

        /// <inheritdoc />
        public void ClearQueryFilters()
        {
            _QueryFilters = Array.Empty<Expression<Func<T, bool>>>();
        }

        /// <inheritdoc />
        public T? ReadFirst(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return SyncBridge.Run(() => ReadFirstAsync(predicate, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public T? ReadFirstOrDefault(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return ReadFirst(predicate, transaction);
        }

        /// <inheritdoc />
        public T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return SyncBridge.Run(() => ReadSingleAsync(predicate, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public T? ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return SyncBridge.Run(() => ReadSingleOrDefaultAsync(predicate, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public IEnumerable<T> ReadMany(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            QueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            QueryModel model = query.BuildModel(true);
            return SyncBridge.Enumerate(query.StreamAsync(model, CancellationToken.None));
        }

        /// <inheritdoc />
        public IEnumerable<T> ReadAll(ITransaction? transaction = null)
        {
            return ReadMany(null, transaction);
        }

        /// <inheritdoc />
        public T? ReadById(object id, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(id);
            return SyncBridge.Run(() => ReadByIdAsync(id, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<T?> ReadFirstAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            QueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return (await query.Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <inheritdoc />
        public Task<T?> ReadFirstOrDefaultAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return ReadFirstAsync(predicate, transaction, token);
        }

        /// <inheritdoc />
        public async Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            List<T> results = (await NewQuery(transaction).Where(predicate).Take(2).ExecuteAsync(token).ConfigureAwait(false)).ToList();
            if (results.Count == 0) throw new InvalidOperationException("Sequence contains no matching element.");
            if (results.Count > 1) throw new InvalidOperationException("Sequence contains more than one matching element.");
            return results[0];
        }

        /// <inheritdoc />
        public async Task<T?> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            List<T> results = (await NewQuery(transaction).Where(predicate).Take(2).ExecuteAsync(token).ConfigureAwait(false)).ToList();
            if (results.Count > 1) throw new InvalidOperationException("Sequence contains more than one matching element.");
            return results.FirstOrDefault();
        }

        /// <inheritdoc />
        public IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            QueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query.ExecuteAsyncEnumerable(token);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<T> ReadAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            return ReadManyAsync(null, transaction, token);
        }

        /// <inheritdoc />
        public async Task<T?> ReadByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(id);
            return (await NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <inheritdoc />
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).Any();
        }

        /// <inheritdoc />
        public bool ExistsById(object id, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(id);
            return NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).Any();
        }

        /// <inheritdoc />
        public Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).AnyAsync(token);
        }

        /// <inheritdoc />
        public Task<bool> ExistsByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(id);
            return NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).AnyAsync(token);
        }

        /// <inheritdoc />
        public long Count(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return Filtered(predicate, transaction).Count();
        }

        /// <inheritdoc />
        public Task<long> CountAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Filtered(predicate, transaction).CountAsync(token);
        }

        /// <inheritdoc />
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).Max(selector);
        }

        /// <inheritdoc />
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).Min(selector);
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).Average(selector);
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).Sum(selector);
        }

        /// <inheritdoc />
        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).MaxAsync(selector, token);
        }

        /// <inheritdoc />
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).MinAsync(selector, token);
        }

        /// <inheritdoc />
        public Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).AverageAsync(selector, token);
        }

        /// <inheritdoc />
        public Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(selector);
            return Filtered(predicate, transaction).SumAsync(selector, token);
        }

        /// <inheritdoc />
        public T Create(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return SyncBridge.Run(() => CreateAsync(entity, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            List<T> list = MaterializeEntities(entities);
            return SyncBridge.Run(() => CreateManyCoreAsync(list, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public async Task<T> CreateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            PrepareForInsert(entity);
            ClearGeneratedValue(entity);
            await Backend.InsertAsync(Metadata, entity, ResolveTransaction(transaction), token).ConfigureAwait(false);
            return entity;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            List<T> list = MaterializeEntities(entities);
            return await CreateManyCoreAsync(list, transaction, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public T Update(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return SyncBridge.Run(() => UpdateCoreAsync(entity, transaction, true, CancellationToken.None));
        }

        /// <inheritdoc />
        public int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateAction);
            return SyncBridge.Run(() => RunInTransactionAsync(transaction, async context =>
            {
                List<T> entities = (await NewQuery(context).Where(predicate).ExecuteAsync(CancellationToken.None).ConfigureAwait(false)).ToList();
                foreach (T entity in entities)
                {
                    updateAction(entity);
                    await UpdateCoreAsync(entity, context, true, CancellationToken.None).ConfigureAwait(false);
                }

                return entities.Count;
            }, CancellationToken.None));
        }

        /// <inheritdoc />
        public int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(field);
            return SyncBridge.Run(() => UpdateFieldAsync(predicate, field, value, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        public Task<T> UpdateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return UpdateCoreAsync(entity, transaction, false, token);
        }

        /// <inheritdoc />
        public Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateAction);
            return RunInTransactionAsync(transaction, async context =>
            {
                List<T> entities = (await NewQuery(context).Where(predicate).ExecuteAsync(token).ConfigureAwait(false)).ToList();
                foreach (T entity in entities)
                {
                    token.ThrowIfCancellationRequested();
                    await updateAction(entity).ConfigureAwait(false);
                    await UpdateCoreAsync(entity, context, false, token).ConfigureAwait(false);
                }

                return entities.Count;
            }, token);
        }

        /// <inheritdoc />
        /// <exception cref="ArgumentException">Thrown when the field selector does not reference a mapped column.</exception>
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.BatchUpdate"/>.</exception>
        public async Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(field);
            ThrowIfDisposed();
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.BatchUpdate, "UpdateField");

            QueryBuilder<T> query = NewQuery(transaction);
            query.Where(predicate);
            QueryModel model = query.BuildModel(false);
            QueryNormalizer normalizer = CreateNormalizer();
            normalizer.Bind(field.Parameters[0], model.Source);
            ColumnNode column = normalizer.ResolveColumn(field.Body)
                ?? throw new ArgumentException("The field selector must reference a mapped column, for example x => x.Name.", nameof(field));

            List<FieldAssignment> assignments = new List<FieldAssignment>
            {
                new FieldAssignment(column.Column, new ValueNode(value, column.Column, column.Column.PropertyType))
            };
            AppendVersionBump(assignments, model.Source, column.Column);
            ValidateAssignments(assignments);
            return await Backend.UpdateAsync(model, assignments, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateExpression);
            return SyncBridge.Run(() => BatchUpdateAsync(predicate, updateExpression, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the expression is not a member-init expression, assigns an unmapped or key column, or the backend lacks <see cref="RepositoryCapabilities.BatchUpdate"/>.</exception>
        public async Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateExpression);
            ThrowIfDisposed();
            if (updateExpression.Body is not MemberInitExpression memberInit)
                throw new NotSupportedException("BatchUpdate requires a member-init expression, for example x => new T { Name = \"value\" }.");
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.BatchUpdate, "BatchUpdate");

            QueryBuilder<T> query = NewQuery(transaction);
            query.Where(predicate);
            QueryModel model = query.BuildModel(false);
            QueryNormalizer normalizer = CreateNormalizer();
            normalizer.Bind(updateExpression.Parameters[0], model.Source);

            List<FieldAssignment> assignments = new List<FieldAssignment>();
            ColumnMetadata? explicitVersion = null;
            foreach (MemberBinding binding in memberInit.Bindings)
            {
                if (binding is not MemberAssignment assignment || assignment.Member is not PropertyInfo property)
                    throw new NotSupportedException("BatchUpdate bindings must assign properties.");
                ColumnMetadata column = Metadata.FindColumn(property)
                    ?? throw new NotSupportedException("Property '" + property.Name + "' is not a mapped column.");
                if (column.IsPrimaryKey) throw new NotSupportedException("BatchUpdate cannot modify primary key column '" + column.Name + "'.");
                if (column.IsVersion) explicitVersion = column;

                QueryNode value = ExpressionEvaluator.IsEvaluable(assignment.Expression)
                    ? new ValueNode(ExpressionEvaluator.Evaluate(assignment.Expression), column, column.PropertyType)
                    : normalizer.Normalize(assignment.Expression);
                assignments.Add(new FieldAssignment(column, value));
            }

            if (assignments.Count == 0) throw new NotSupportedException("BatchUpdate requires at least one assignment.");
            if (explicitVersion == null) AppendVersionBump(assignments, model.Source, null);
            ValidateAssignments(assignments);
            return await Backend.UpdateAsync(model, assignments, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public bool Delete(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            object?[] key = RequireKeyValues(entity);
            return SyncBridge.Run(() => DeleteByKeyAsync(key, transaction, CancellationToken.None)) > 0;
        }

        /// <inheritdoc />
        public bool DeleteById(object id, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(id);
            object?[] key = Metadata.SplitKey(id);
            return SyncBridge.Run(() => DeleteByKeyAsync(key, transaction, CancellationToken.None)) > 0;
        }

        /// <inheritdoc />
        public int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).Delete();
        }

        /// <inheritdoc />
        public int DeleteAll(ITransaction? transaction = null)
        {
            return NewQuery(transaction).Delete();
        }

        /// <inheritdoc />
        public int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            return DeleteMany(predicate, transaction);
        }

        /// <inheritdoc />
        public async Task<bool> DeleteAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return await DeleteByKeyAsync(RequireKeyValues(entity), transaction, token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public async Task<bool> DeleteByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(id);
            return await DeleteByKeyAsync(Metadata.SplitKey(id), transaction, token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).DeleteAsync(token);
        }

        /// <inheritdoc />
        public Task<int> DeleteAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            return NewQuery(transaction).DeleteAsync(token);
        }

        /// <inheritdoc />
        public Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            return DeleteManyAsync(predicate, transaction, token);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Upsert"/>.</exception>
        public T Upsert(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return SyncBridge.Run(() => UpsertAsync(entity, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Upsert"/>.</exception>
        public IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            List<T> list = MaterializeEntities(entities);
            return SyncBridge.Run(() => UpsertManyCoreAsync(list, transaction, CancellationToken.None));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Upsert"/>.</exception>
        public async Task<T> UpsertAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Upsert, "Upsert");
            if (HasUnsetGeneratedKey(entity)) return await CreateAsync(entity, transaction, token).ConfigureAwait(false);
            return await RunInTransactionAsync(transaction, context => UpsertCoreAsync(entity, context, token), token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Upsert"/>.</exception>
        public async Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            List<T> list = MaterializeEntities(entities);
            return await UpsertManyCoreAsync(list, transaction, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public IQueryBuilder<T> Query(ITransaction? transaction = null)
        {
            ThrowIfDisposed();
            return NewQuery(transaction);
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Transactions"/>.</exception>
        public ITransaction BeginTransaction()
        {
            return SyncBridge.Run(() => BeginTransactionAsync(CancellationToken.None));
        }

        /// <inheritdoc />
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Transactions"/>.</exception>
        public Task<ITransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Transactions, "BeginTransaction");
            return Backend.BeginTransactionAsync(token);
        }

        /// <summary>
        /// Disposes the repository. The backend is not disposed.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Creates the query builder used by <see cref="Query"/> and every predicate-based operation. Override to return a
        /// builder that pushes Select or GroupBy down to the backend.
        /// </summary>
        /// <param name="transaction">Explicit transaction; may be null.</param>
        /// <returns>A new query builder. Never null.</returns>
        protected virtual QueryBuilder<T> CreateQueryBuilder(ITransaction? transaction)
        {
            return new QueryBuilder<T>(this, transaction);
        }

        /// <summary>
        /// Resolves the transaction an operation runs in: the explicit transaction, otherwise the ambient
        /// <see cref="TransactionScope.Current"/> transaction when it is not completed and <see cref="AcceptsAmbientTransaction"/>
        /// accepts it, otherwise null.
        /// </summary>
        /// <param name="transaction">Explicit transaction; may be null.</param>
        /// <returns>The transaction or null.</returns>
        protected internal virtual ITransaction? ResolveTransaction(ITransaction? transaction)
        {
            if (transaction != null) return transaction;
            TransactionScope? scope = TransactionScope.Current;
            if (scope == null) return null;
            ITransaction ambient;
            try
            {
                ambient = scope.Transaction;
            }
            catch (InvalidOperationException)
            {
                return null;
            }

            if (ambient.IsCompleted || !AcceptsAmbientTransaction(ambient)) return null;
            return ambient;
        }

        /// <summary>
        /// Determines whether an ambient transaction belongs to this repository's backend. The default accepts every
        /// transaction; backends override it so a scope started by another backend is ignored, as SQL repositories ignore
        /// scopes of other providers.
        /// </summary>
        /// <param name="transaction">Ambient transaction. Never null.</param>
        /// <returns>True to use the transaction.</returns>
        protected virtual bool AcceptsAmbientTransaction(ITransaction transaction)
        {
            return true;
        }

        /// <summary>
        /// Returns the query text reported by <see cref="IQueryBuilder{T}.Query"/> and <see cref="IDurableResult{T}.Query"/>.
        /// The default is a readable description from <see cref="QueryModelFormatter"/>; backends with a native query
        /// language can return it instead.
        /// </summary>
        /// <param name="model">Query model. Never null.</param>
        /// <returns>The query text. Never null.</returns>
        protected internal virtual string DescribeQuery(QueryModel model)
        {
            return QueryModelFormatter.Describe(model);
        }

        /// <summary>
        /// Creates a normalizer configured with <see cref="RepositoryOptions.StringMatching"/>.
        /// </summary>
        /// <returns>A new normalizer.</returns>
        protected internal QueryNormalizer CreateNormalizer()
        {
            return new QueryNormalizer(Options.StringMatching);
        }

        /// <summary>
        /// Creates the loader used for Include/ThenInclude.
        /// </summary>
        /// <returns>A new loader.</returns>
        protected internal virtual BackendIncludeLoader CreateIncludeLoader()
        {
            return new BackendIncludeLoader(Backend) { ChunkSize = _IncludeChunkSize };
        }

        /// <summary>
        /// Deletes the rows a model selects, or sets their soft-delete marker when the entity has one.
        /// </summary>
        /// <param name="model">Rows to delete. Never null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows affected.</returns>
        protected internal Task<int> DeleteModelAsync(QueryModel model, CancellationToken token)
        {
            ThrowIfDisposed();
            ColumnMetadata? softDelete = Metadata.SoftDeleteColumn;
            if (softDelete == null) return Backend.DeleteAsync(model, token);
            List<FieldAssignment> assignments = new List<FieldAssignment>
            {
                new FieldAssignment(softDelete, new ValueNode(QueryConditions.SoftDeleteValue(softDelete), softDelete, softDelete.PropertyType))
            };
            return Backend.UpdateAsync(model, assignments, token);
        }

        /// <summary>
        /// Releases resources.
        /// </summary>
        /// <param name="disposing">True when called from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            Interlocked.Exchange(ref _Disposed, 1);
        }

        /// <summary>
        /// Throws when the repository is disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when disposed.</exception>
        protected void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(GetType().Name);
        }

        private QueryBuilder<T> NewQuery(ITransaction? transaction)
        {
            return CreateQueryBuilder(transaction);
        }

        private QueryBuilder<T> Filtered(Expression<Func<T, bool>>? predicate, ITransaction? transaction)
        {
            QueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query;
        }

        private async Task<IEnumerable<T>> CreateManyCoreAsync(List<T> list, ITransaction? transaction, CancellationToken token)
        {
            ThrowIfDisposed();
            if (list.Count == 0) return list;
            foreach (T entity in list) PrepareForInsert(entity);
            await RunInTransactionAsync(transaction, async context =>
            {
                foreach (T entity in list)
                {
                    token.ThrowIfCancellationRequested();
                    ClearGeneratedValue(entity);
                    await Backend.InsertAsync(Metadata, entity, context, token).ConfigureAwait(false);
                }

                return 0;
            }, token).ConfigureAwait(false);
            return list;
        }

        private async Task<T> UpdateCoreAsync(T entity, ITransaction? transaction, bool synchronousResolver, CancellationToken token)
        {
            ThrowIfDisposed();
            object?[] key = RequireKeyValues(entity);
            if (_UpdateColumns.Count == 0) throw new InvalidOperationException("Entity " + typeof(T).Name + " has no updatable columns.");

            ColumnMetadata? version = Metadata.VersionColumn;
            object? originalVersion = version?.GetValue(entity);
            object? newVersion = version != null ? Metadata.VersionInfo!.IncrementVersion(originalVersion!) : null;
            T values = CopyColumns(entity);
            if (version != null) version.SetValue(values, newVersion);

            QuerySource source = new QuerySource(Metadata, Metadata.TableName);
            QueryNode condition = QueryConditions.Key(source, key);
            if (version != null) condition = new LogicalNode(LogicalOperator.And, condition, QueryConditions.Equal(source, version, originalVersion));

            int rows = await Backend.ReplaceAsync(Metadata, values, condition, source, ResolveTransaction(transaction), token).ConfigureAwait(false);
            if (rows > 0)
            {
                if (version != null) version.SetValue(entity, newVersion);
                return entity;
            }

            if (version == null)
                throw new InvalidOperationException("No rows were affected during update for entity with key " + FormatKey(key) + ".");

            T? current = (await NewQuery(transaction).WhereKey(key).Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
            T resolved = synchronousResolver
                ? ResolveConflict(entity, current, key)
                : await ResolveConflictAsync(entity, current, key).ConfigureAwait(false);
            return await UpdateCoreAsync(resolved, transaction, synchronousResolver, token).ConfigureAwait(false);
        }

        private Task<int> DeleteByKeyAsync(object?[] key, ITransaction? transaction, CancellationToken token)
        {
            ThrowIfDisposed();
            QuerySource source = new QuerySource(Metadata, Metadata.TableName);
            QueryModel model = new QueryModel(source)
            {
                Filter = QueryConditions.Key(source, key),
                Transaction = ResolveTransaction(transaction)
            };
            return DeleteModelAsync(model, token);
        }

        private async Task<T> UpsertCoreAsync(T entity, ITransaction? transaction, CancellationToken token)
        {
            PrepareForInsert(entity);
            object?[] key = RequireKeyValues(entity);
            QuerySource source = new QuerySource(Metadata, Metadata.TableName);
            QueryModel probe = new QueryModel(source)
            {
                Filter = QueryConditions.Key(source, key),
                Take = 1,
                Transaction = ResolveTransaction(transaction)
            };

            bool exists = await Backend.CountAsync(probe, token).ConfigureAwait(false) > 0;
            if (exists && _UpdateColumns.Count > 0)
            {
                T values = CopyColumns(entity);
                ColumnMetadata? version = Metadata.VersionColumn;
                if (version != null) version.SetValue(values, Metadata.VersionInfo!.IncrementVersion(version.GetValue(entity)!));
                await Backend.ReplaceAsync(Metadata, values, QueryConditions.Key(source, key), source, probe.Transaction, token).ConfigureAwait(false);
            }
            else if (!exists)
            {
                await Backend.InsertAsync(Metadata, entity, probe.Transaction, token).ConfigureAwait(false);
            }

            T? stored = (await NewQuery(transaction).WhereKey(key).Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
            if (stored != null)
            {
                foreach (ColumnMetadata column in Metadata.Columns) column.SetValue(entity, column.GetValue(stored));
            }

            return entity;
        }

        private async Task<IEnumerable<T>> UpsertManyCoreAsync(List<T> list, ITransaction? transaction, CancellationToken token)
        {
            QueryCapabilityValidator.Require(Capabilities, RepositoryCapabilities.Upsert, "Upsert");
            if (list.Count == 0) return list;
            await RunInTransactionAsync(transaction, async context =>
            {
                foreach (T entity in list)
                {
                    token.ThrowIfCancellationRequested();
                    await UpsertAsync(entity, context, token).ConfigureAwait(false);
                }

                return 0;
            }, token).ConfigureAwait(false);
            return list;
        }

        private async Task<TResult> RunInTransactionAsync<TResult>(ITransaction? transaction, Func<ITransaction?, Task<TResult>> work, CancellationToken token)
        {
            ThrowIfDisposed();
            ITransaction? existing = ResolveTransaction(transaction);
            if (existing != null) return await work(existing).ConfigureAwait(false);
            if ((Capabilities & RepositoryCapabilities.Transactions) != RepositoryCapabilities.Transactions)
                return await work(null).ConfigureAwait(false);

            ITransaction owned = await Backend.BeginTransactionAsync(token).ConfigureAwait(false);
            await using (owned.ConfigureAwait(false))
            {
                TResult result = await work(owned).ConfigureAwait(false);
                await owned.CommitAsync(token).ConfigureAwait(false);
                return result;
            }
        }

        private void AppendVersionBump(List<FieldAssignment> assignments, QuerySource source, ColumnMetadata? assignedColumn)
        {
            ColumnMetadata? version = Metadata.VersionColumn;
            if (version == null || ReferenceEquals(version, assignedColumn)) return;
            switch (Metadata.VersionInfo!.Type)
            {
                case VersionColumnType.Integer:
                    assignments.Add(new FieldAssignment(version, new ArithmeticNode(
                        ArithmeticOperator.Add,
                        new ColumnNode(source, version),
                        new ValueNode(1, null, typeof(int)),
                        false,
                        version.PropertyType)));
                    break;
                case VersionColumnType.Timestamp:
                    assignments.Add(new FieldAssignment(version, new ValueNode(DateTime.UtcNow, version, version.PropertyType)));
                    break;
                case VersionColumnType.Guid:
                    assignments.Add(new FieldAssignment(version, new ValueNode(Guid.NewGuid(), version, version.PropertyType)));
                    break;
            }
        }

        private void ValidateAssignments(List<FieldAssignment> assignments)
        {
            foreach (FieldAssignment assignment in assignments) QueryCapabilityValidator.Validate(assignment.Value, Capabilities);
        }

        private T ResolveConflict(T incoming, T? current, object?[] key)
        {
            if (current == null)
                throw new OptimisticConcurrencyException("Entity " + typeof(T).Name + " with key " + FormatKey(key) + " was deleted by another process.");

            T original = CopyColumns(incoming);
            bool resolved;
            T result;
            try
            {
                resolved = _ConflictResolver.TryResolveConflict(current, incoming, original, _ConflictResolver.DefaultStrategy, out result);
            }
            catch (OptimisticConcurrencyException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new OptimisticConcurrencyException("Error during conflict resolution for " + typeof(T).Name + " with key " + FormatKey(key) + ": " + e.Message, e);
            }

            if (!resolved || result == null)
                throw new OptimisticConcurrencyException("Optimistic concurrency conflict detected for " + typeof(T).Name + " with key " + FormatKey(key) + " and it could not be resolved.");

            Metadata.VersionColumn!.SetValue(result, Metadata.VersionColumn.GetValue(current));
            return result;
        }

        private async Task<T> ResolveConflictAsync(T incoming, T? current, object?[] key)
        {
            if (current == null)
                throw new OptimisticConcurrencyException("Entity " + typeof(T).Name + " with key " + FormatKey(key) + " was deleted by another process.");

            T original = CopyColumns(incoming);
            TryResolveConflictResult<T> outcome;
            try
            {
                outcome = await _ConflictResolver.TryResolveConflictAsync(current, incoming, original, _ConflictResolver.DefaultStrategy).ConfigureAwait(false);
            }
            catch (OptimisticConcurrencyException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new OptimisticConcurrencyException("Error during conflict resolution for " + typeof(T).Name + " with key " + FormatKey(key) + ": " + e.Message, e);
            }

            if (!outcome.Success || outcome.ResolvedEntity == null)
                throw new OptimisticConcurrencyException("Optimistic concurrency conflict detected for " + typeof(T).Name + " with key " + FormatKey(key) + " and it could not be resolved.");

            Metadata.VersionColumn!.SetValue(outcome.ResolvedEntity, Metadata.VersionColumn.GetValue(current));
            return outcome.ResolvedEntity;
        }

        private T CopyColumns(T source)
        {
            T copy = new T();
            foreach (ColumnMetadata column in Metadata.Columns) column.SetValue(copy, column.GetValue(source));
            return copy;
        }

        private static List<T> MaterializeEntities(IEnumerable<T> entities)
        {
            ArgumentNullException.ThrowIfNull(entities);
            List<T> list = entities as List<T> ?? entities.ToList();
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i] == null) throw new ArgumentNullException(nameof(entities), "Entity at index " + i + " is null.");
            }

            return list;
        }

        private void PrepareForInsert(T entity)
        {
            foreach (ColumnMetadata column in Metadata.Columns)
            {
                DefaultValueProviderInfo? defaultValue = column.DefaultValue;
                if (defaultValue == null) continue;
                object? current = column.GetValue(entity);
                if (defaultValue.Provider.ShouldApply(current, column.PropertyType))
                    column.SetValue(entity, defaultValue.Provider.GetDefaultValue(column.Property, entity));
            }

            ColumnMetadata? version = Metadata.VersionColumn;
            if (version != null)
            {
                object? value = version.GetValue(entity);
                if (IsUnsetVersion(value)) version.SetValue(entity, Metadata.VersionInfo!.GetDefaultVersion());
            }
        }

        private void ClearGeneratedValue(T entity)
        {
            // SQL repositories omit auto-increment columns from INSERT, so any value the caller set is replaced by the
            // generated one; clearing it gives backends the same contract (an unset value means "generate").
            ColumnMetadata? generated = Metadata.AutoIncrementColumn;
            if (generated != null) generated.SetValue(entity, null);
        }

        private static bool IsUnsetVersion(object? value)
        {
            switch (value)
            {
                case null: return true;
                case int i: return i == 0;
                case long l: return l == 0;
                case short s: return s == 0;
                case byte b: return b == 0;
                case DateTime dt: return dt == DateTime.MinValue;
                case Guid g: return g == Guid.Empty;
                case byte[] bytes: return bytes.Length == 0;
                default: return false;
            }
        }

        private bool HasUnsetGeneratedKey(T entity)
        {
            ColumnMetadata? generated = Metadata.AutoIncrementColumn;
            if (generated == null || !generated.IsPrimaryKey) return false;
            object? value = generated.GetValue(entity);
            return value == null || IsUnsetVersion(value) || (value is string s && s.Length == 0);
        }

        private object?[] RequireKeyValues(T entity)
        {
            object?[] values = new object?[Metadata.KeyColumns.Count];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = Metadata.KeyColumns[i].GetValue(entity);
                if (values[i] == null)
                    throw new InvalidOperationException("Primary key column '" + Metadata.KeyColumns[i].Name + "' of " + typeof(T).Name + " is null.");
            }

            return values;
        }

        private static string FormatKey(object?[] key)
        {
            return key.Length == 1 ? Convert.ToString(key[0], CultureInfo.InvariantCulture) ?? "null" : "(" + string.Join(", ", key.Select(k => Convert.ToString(k, CultureInfo.InvariantCulture))) + ")";
        }

        #endregion
    }
}
