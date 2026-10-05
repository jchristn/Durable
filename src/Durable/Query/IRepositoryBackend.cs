namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// The small storage contract a non-SQL backend (document store, search engine, graph store, in-memory) implements to
    /// get the whole <see cref="IRepository{T}"/> surface from <see cref="RepositoryBase{T}"/>: query, count, aggregate,
    /// insert, conditional update, set-based update, delete and transactions, all expressed with backend-neutral
    /// <see cref="QueryModel"/>s and <see cref="QueryNode"/>s. One backend instance serves every entity type, which is how
    /// Include loads related entities. Value conversion is the backend's job: <see cref="ValueNode.Column"/> names the
    /// column a value is compared with, and <see cref="ColumnMetadata.Converter"/> describes per-property converters.
    /// Thread safety: implementations must be safe for concurrent use by multiple repositories.
    /// </summary>
    public interface IRepositoryBackend
    {
        /// <summary>
        /// Gets the optional features the backend supports. Operations needing a missing capability fail before reaching the backend.
        /// </summary>
        RepositoryCapabilities Capabilities { get; }

        /// <summary>
        /// Streams the entities matching a model (filter, ordering, paging, distinct), materialized as instances of
        /// <see cref="QueryModel.Metadata"/>'s entity type.
        /// </summary>
        /// <param name="model">Query. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The entities.</returns>
        IAsyncEnumerable<object> QueryAsync(QueryModel model, CancellationToken token);

        /// <summary>
        /// Counts the entities matching a model's filter (ordering and paging are applied when present).
        /// </summary>
        /// <param name="model">Query. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The count.</returns>
        Task<long> CountAsync(QueryModel model, CancellationToken token);

        /// <summary>
        /// Computes Sum, Average, Min or Max of a value over the entities matching a model's filter.
        /// </summary>
        /// <param name="model">Query. Must not be null.</param>
        /// <param name="function">Aggregate: <see cref="AggregateFunction.Sum"/>, <see cref="AggregateFunction.Average"/>, <see cref="AggregateFunction.Min"/> or <see cref="AggregateFunction.Max"/>.</param>
        /// <param name="operand">Aggregated value over the model's source. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>
        /// The aggregate, or null when no entity matches (Sum of no rows is reported as null; callers map it to zero).
        /// Null operand values are ignored, as in SQL. Min and Max return the operand's CLR value (converted back through the
        /// column's converter when the operand is a column); <see cref="RepositoryBase{T}"/> converts numeric results to the
        /// caller's type.
        /// </returns>
        Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token);

        /// <summary>
        /// Inserts an entity and writes generated values (auto-increment keys) back to it. An auto-increment column holding
        /// its unset value (null or zero) is generated; a set value is stored as given (used by upsert of an explicit key).
        /// <see cref="RepositoryBase{T}"/> clears auto-increment values before Create, matching SQL repositories, which
        /// always generate them.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="transaction">Transaction; null for none.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the key already exists.</exception>
        Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token);

        /// <summary>
        /// Replaces the stored values of the entities matching a condition (the key, plus the expected version when
        /// optimistic concurrency applies) with the values of <paramref name="entity"/>.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="entity">Entity holding the new values. Must not be null.</param>
        /// <param name="condition">Condition over <paramref name="source"/>. Must not be null.</param>
        /// <param name="source">Source the condition refers to. Must not be null.</param>
        /// <param name="transaction">Transaction; null for none.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of entities updated (0 when the condition matched nothing).</returns>
        Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token);

        /// <summary>
        /// Applies column assignments to every entity matching a model's filter.
        /// </summary>
        /// <param name="model">Rows to update. Must not be null.</param>
        /// <param name="assignments">Assignments, evaluated against each row's current values. Must not be null or empty.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of entities updated.</returns>
        Task<int> UpdateAsync(QueryModel model, IReadOnlyList<FieldAssignment> assignments, CancellationToken token);

        /// <summary>
        /// Deletes every entity matching a model's filter.
        /// </summary>
        /// <param name="model">Rows to delete. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The number of entities deleted.</returns>
        Task<int> DeleteAsync(QueryModel model, CancellationToken token);

        /// <summary>
        /// Begins a transaction.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The transaction.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend lacks <see cref="RepositoryCapabilities.Transactions"/>.</exception>
        Task<ITransaction> BeginTransactionAsync(CancellationToken token);
    }
}
