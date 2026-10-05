namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Backend-neutral repository for an entity type.
    /// Every method accepting an <see cref="ITransaction"/> runs inside that transaction when supplied; when null, the
    /// ambient <see cref="TransactionScope.Current"/> is used if present, otherwise the operation runs on its own.
    /// Key arguments (<c>id</c>) are a scalar for single-column keys, or an <see cref="object"/> array with one value
    /// per key column (in key order) for composite keys.
    /// Query filters registered with <see cref="AddQueryFilter"/> and soft-delete filtering apply to all predicate-based
    /// reads and writes; key-based <see cref="Update"/> and <see cref="Delete"/> of a specific entity are not filtered.
    /// Thread safety: implementations are safe for concurrent use once configured; configuration members
    /// (<see cref="AddQueryFilter"/>, <see cref="ClearQueryFilters"/>) must not be called concurrently with operations.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public interface IRepository<T> : IDisposable where T : class, new()
    {
        #region Configuration

        /// <summary>
        /// Gets the cached mapping metadata for <typeparamref name="T"/>. Never null.
        /// </summary>
        EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the optional features this repository's backend supports. Operations that need a missing capability throw
        /// <see cref="NotSupportedException"/> when called. SQL repositories support <see cref="RepositoryCapabilities.All"/>.
        /// </summary>
        RepositoryCapabilities Capabilities { get; }

        /// <summary>
        /// Gets or sets the conflict resolver used when an update hits a version conflict (entities with a version column).
        /// Default: a resolver that throws <see cref="OptimisticConcurrencyException"/>. Never null.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when set to null.</exception>
        IConcurrencyConflictResolver<T> ConflictResolver { get; set; }

        /// <summary>
        /// Gets the registered global query filters. Never null.
        /// </summary>
        IReadOnlyList<Expression<Func<T, bool>>> QueryFilters { get; }

        /// <summary>
        /// Registers a filter that is combined (AND) with every predicate-based read and write on this repository.
        /// Captured variables are evaluated each time a query is built, so a filter such as
        /// <c>x =&gt; x.TenantId == tenantContext.TenantId</c> follows the current tenant.
        /// Bypass per query with <see cref="IQueryBuilder{T}.IgnoreQueryFilters"/>.
        /// </summary>
        /// <param name="filter">Filter predicate. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when filter is null.</exception>
        void AddQueryFilter(Expression<Func<T, bool>> filter);

        /// <summary>
        /// Removes all registered query filters. Soft-delete filtering is unaffected.
        /// </summary>
        void ClearQueryFilters();

        #endregion

        #region Read

        /// <summary>
        /// Reads the first entity matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The first matching entity, or null when none match.</returns>
        T? ReadFirst(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Reads the first entity matching the predicate. Equivalent to <see cref="ReadFirst"/>.
        /// </summary>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The first matching entity, or null.</returns>
        T? ReadFirstOrDefault(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Reads exactly one entity matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The single matching entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches.</exception>
        T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction? transaction = null);

        /// <summary>
        /// Reads at most one entity matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The matching entity, or null when none match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches.</exception>
        T? ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction? transaction = null);

        /// <summary>
        /// Reads all entities matching the predicate. Results are streamed; enumerate once.
        /// </summary>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>Matching entities. Never null.</returns>
        IEnumerable<T> ReadMany(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Reads all entities. Results are streamed; enumerate once.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>All entities. Never null.</returns>
        IEnumerable<T> ReadAll(ITransaction? transaction = null);

        /// <summary>
        /// Reads an entity by primary key.
        /// </summary>
        /// <param name="id">Key value, or an object array for composite keys. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The entity, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="ArgumentException">Thrown when a composite key is supplied with the wrong number of values.</exception>
        T? ReadById(object id, ITransaction? transaction = null);

        /// <summary>
        /// Reads the first entity matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The first matching entity, or null.</returns>
        Task<T?> ReadFirstAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Reads the first entity matching the predicate. Equivalent to <see cref="ReadFirstAsync"/>.
        /// </summary>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The first matching entity, or null.</returns>
        Task<T?> ReadFirstOrDefaultAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Reads exactly one entity matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The single matching entity.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when zero or more than one entity matches.</exception>
        Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Reads at most one entity matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The matching entity, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when more than one entity matches.</exception>
        Task<T?> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Streams entities matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>An async stream of matching entities.</returns>
        IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Streams all entities.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>An async stream of all entities.</returns>
        IAsyncEnumerable<T> ReadAllAsync(ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Reads an entity by primary key.
        /// </summary>
        /// <param name="id">Key value, or an object array for composite keys. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The entity, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        Task<T?> ReadByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Exists-and-Aggregates

        /// <summary>
        /// Determines whether any entity matches the predicate.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when at least one entity matches.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        bool Exists(Expression<Func<T, bool>> predicate, ITransaction? transaction = null);

        /// <summary>
        /// Determines whether an entity with the key exists.
        /// </summary>
        /// <param name="id">Key value, or an object array for composite keys. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when the entity exists.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        bool ExistsById(object id, ITransaction? transaction = null);

        /// <summary>
        /// Determines whether any entity matches the predicate.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True when at least one entity matches.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Determines whether an entity with the key exists.
        /// </summary>
        /// <param name="id">Key value, or an object array for composite keys. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True when the entity exists.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        Task<bool> ExistsByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Counts entities matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate; null counts all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The count.</returns>
        long Count(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Counts entities matching the predicate.
        /// </summary>
        /// <param name="predicate">Predicate; null counts all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The count.</returns>
        Task<long> CountAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Returns the maximum of the selected value over matching entities.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The maximum, or default when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Returns the minimum of the selected value over matching entities.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The minimum, or default when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Returns the average of the selected numeric value over matching entities.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The average, or zero when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        decimal Average<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Returns the sum of the selected numeric value over matching entities.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The sum, or zero when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null);

        /// <summary>
        /// Returns the maximum of the selected value over matching entities.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The maximum, or default when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Returns the minimum of the selected value over matching entities.
        /// </summary>
        /// <typeparam name="TResult">Value type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The minimum, or default when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Returns the average of the selected numeric value over matching entities.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The average, or zero when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Returns the sum of the selected numeric value over matching entities.
        /// </summary>
        /// <typeparam name="TProperty">Numeric type.</typeparam>
        /// <param name="selector">Value selector. Must not be null.</param>
        /// <param name="predicate">Predicate; null matches all rows.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The sum, or zero when no rows match.</returns>
        /// <exception cref="ArgumentNullException">Thrown when selector is null.</exception>
        Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Create

        /// <summary>
        /// Inserts an entity. Database-generated keys, versions and default values are written back to the instance.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The same instance, updated with generated values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        T Create(T entity, ITransaction? transaction = null);

        /// <summary>
        /// Inserts entities in batches. Generated keys are written back to each instance in input order.
        /// Runs in a single transaction when none is supplied.
        /// </summary>
        /// <param name="entities">Entities. Must not be null and must not contain nulls.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The inserted instances in input order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null or contains null.</exception>
        IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction? transaction = null);

        /// <summary>
        /// Inserts an entity. Database-generated keys, versions and default values are written back to the instance.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The same instance, updated with generated values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        Task<T> CreateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Inserts entities in batches. Generated keys are written back to each instance in input order.
        /// Runs in a single transaction when none is supplied.
        /// </summary>
        /// <param name="entities">Entities. Must not be null and must not contain nulls.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The inserted instances in input order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null or contains null.</exception>
        Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Update

        /// <summary>
        /// Updates an entity by primary key. With a version column, the update succeeds only when the stored version
        /// matches; on conflict the configured resolver decides the outcome.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The updated instance (with its new version, if any).</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="OptimisticConcurrencyException">Thrown when a version conflict cannot be resolved.</exception>
        /// <exception cref="InvalidOperationException">Thrown when no row matches the key.</exception>
        T Update(T entity, ITransaction? transaction = null);

        /// <summary>
        /// Reads matching entities, applies an action to each and updates them individually.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="updateAction">Mutation applied to each entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The number of entities updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateAction is null.</exception>
        int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction? transaction = null);

        /// <summary>
        /// Sets one column to a value on all matching rows with a single statement.
        /// </summary>
        /// <typeparam name="TField">Column type.</typeparam>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="field">Column selector. Must not be null.</param>
        /// <param name="value">New value; may be null for nullable columns.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The number of rows updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or field is null.</exception>
        int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null);

        /// <summary>
        /// Updates an entity by primary key.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The updated instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="OptimisticConcurrencyException">Thrown when a version conflict cannot be resolved.</exception>
        /// <exception cref="InvalidOperationException">Thrown when no row matches the key.</exception>
        Task<T> UpdateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Reads matching entities, applies an asynchronous action to each and updates them individually.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="updateAction">Mutation applied to each entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of entities updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateAction is null.</exception>
        Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Sets one column to a value on all matching rows with a single statement.
        /// </summary>
        /// <typeparam name="TField">Column type.</typeparam>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="field">Column selector. Must not be null.</param>
        /// <param name="value">New value; may be null for nullable columns.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or field is null.</exception>
        Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Updates matching rows with a single statement using a member-init expression,
        /// for example <c>x =&gt; new Person { Salary = x.Salary * 1.1m, Department = "Ops" }</c>.
        /// Assignments may reference the current row.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="updateExpression">Member-init expression. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The number of rows updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateExpression is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the update expression is not a member-init expression.</exception>
        int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null);

        /// <summary>
        /// Updates matching rows with a single statement using a member-init expression.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="updateExpression">Member-init expression. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows updated.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate or updateExpression is null.</exception>
        Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Delete

        /// <summary>
        /// Deletes an entity by its key (soft-deletes when the entity has a <see cref="SoftDeleteAttribute"/> column).
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when a row was affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        bool Delete(T entity, ITransaction? transaction = null);

        /// <summary>
        /// Deletes an entity by key.
        /// </summary>
        /// <param name="id">Key value, or an object array for composite keys. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>True when a row was affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        bool DeleteById(object id, ITransaction? transaction = null);

        /// <summary>
        /// Deletes all matching rows with a single statement.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction? transaction = null);

        /// <summary>
        /// Deletes all rows visible through the registered query filters.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The number of rows affected.</returns>
        int DeleteAll(ITransaction? transaction = null);

        /// <summary>
        /// Deletes all matching rows with a single statement. Equivalent to <see cref="DeleteMany"/>.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction? transaction = null);

        /// <summary>
        /// Deletes an entity by its key.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True when a row was affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        Task<bool> DeleteAsync(T entity, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Deletes an entity by key.
        /// </summary>
        /// <param name="id">Key value, or an object array for composite keys. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>True when a row was affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        Task<bool> DeleteByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Deletes all matching rows with a single statement.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Deletes all rows visible through the registered query filters.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows affected.</returns>
        Task<int> DeleteAllAsync(ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Deletes all matching rows with a single statement. Equivalent to <see cref="DeleteManyAsync"/>.
        /// </summary>
        /// <param name="predicate">Predicate. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentNullException">Thrown when predicate is null.</exception>
        Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Upsert

        /// <summary>
        /// Inserts the entity or updates the existing row with the same key, using the backend's native upsert.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The entity as stored.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        T Upsert(T entity, ITransaction? transaction = null);

        /// <summary>
        /// Upserts each entity. Runs in a single transaction when none is supplied.
        /// </summary>
        /// <param name="entities">Entities. Must not be null and must not contain nulls.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>The entities as stored, in input order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null or contains null.</exception>
        IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction? transaction = null);

        /// <summary>
        /// Inserts the entity or updates the existing row with the same key.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The entity as stored.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        Task<T> UpsertAsync(T entity, ITransaction? transaction = null, CancellationToken token = default);

        /// <summary>
        /// Upserts each entity. Runs in a single transaction when none is supplied.
        /// </summary>
        /// <param name="entities">Entities. Must not be null and must not contain nulls.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The entities as stored, in input order.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entities is null or contains null.</exception>
        Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default);

        #endregion

        #region Query-and-Transactions

        /// <summary>
        /// Starts a fluent query.
        /// </summary>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <returns>A new query builder.</returns>
        IQueryBuilder<T> Query(ITransaction? transaction = null);

        /// <summary>
        /// Begins a transaction.
        /// </summary>
        /// <returns>The transaction. Dispose it; an uncommitted transaction rolls back on dispose.</returns>
        /// <exception cref="NotSupportedException">Thrown by backends without transaction support.</exception>
        ITransaction BeginTransaction();

        /// <summary>
        /// Begins a transaction.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The transaction.</returns>
        /// <exception cref="NotSupportedException">Thrown by backends without transaction support.</exception>
        Task<ITransaction> BeginTransactionAsync(CancellationToken token = default);

        #endregion
    }
}
