namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Durable.Query;

    /// <summary>
    /// A backend that forwards to an <see cref="InMemoryBackend"/> but yields before every operation (and every streamed
    /// row), so no call completes synchronously. Used to prove that synchronous repository members do not deadlock when
    /// called under a synchronization context that never pumps.
    /// </summary>
    public sealed class YieldingBackend : IRepositoryBackend
    {
        #region Public-Members

        /// <summary>
        /// Gets the wrapped backend. Never null.
        /// </summary>
        public InMemoryBackend Inner { get; }

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => Inner.Capabilities;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the backend.
        /// </summary>
        /// <param name="inner">Wrapped backend. Must not be null.</param>
        public YieldingBackend(InMemoryBackend inner)
        {
            Inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public async IAsyncEnumerable<object> QueryAsync(QueryModel model, [EnumeratorCancellation] CancellationToken token)
        {
            await Task.Yield();
            await foreach (object item in Inner.QueryAsync(model, token))
            {
                await Task.Yield();
                yield return item;
            }
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(QueryModel model, CancellationToken token)
        {
            await Task.Yield();
            return await Inner.CountAsync(model, token);
        }

        /// <inheritdoc />
        public async Task<object?> AggregateAsync(QueryModel model, AggregateFunction function, QueryNode operand, CancellationToken token)
        {
            await Task.Yield();
            return await Inner.AggregateAsync(model, function, operand, token);
        }

        /// <inheritdoc />
        public async Task InsertAsync(EntityMetadata metadata, object entity, ITransaction? transaction, CancellationToken token)
        {
            await Task.Yield();
            await Inner.InsertAsync(metadata, entity, transaction, token);
        }

        /// <inheritdoc />
        public async Task<int> ReplaceAsync(EntityMetadata metadata, object entity, QueryNode condition, QuerySource source, ITransaction? transaction, CancellationToken token)
        {
            await Task.Yield();
            return await Inner.ReplaceAsync(metadata, entity, condition, source, transaction, token);
        }

        /// <inheritdoc />
        public async Task<int> UpdateAsync(QueryModel model, IReadOnlyList<FieldAssignment> assignments, CancellationToken token)
        {
            await Task.Yield();
            return await Inner.UpdateAsync(model, assignments, token);
        }

        /// <inheritdoc />
        public async Task<int> DeleteAsync(QueryModel model, CancellationToken token)
        {
            await Task.Yield();
            return await Inner.DeleteAsync(model, token);
        }

        /// <inheritdoc />
        public async Task<ITransaction> BeginTransactionAsync(CancellationToken token)
        {
            await Task.Yield();
            return await Inner.BeginTransactionAsync(token);
        }

        #endregion
    }
}
