namespace Durable.InMemory
{
    using System;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// A repository stored in an <see cref="InMemoryBackend"/>: the complete <see cref="IRepository{T}"/> surface from
    /// <see cref="RepositoryBase{T}"/> over in-memory tables. Repositories created over the same backend share data, so
    /// includes, navigation predicates and transactions work across entity types. An ambient
    /// <see cref="TransactionScope"/> is used only when its transaction was created by the same backend.
    /// Thread safety: safe for concurrent use once configured (see <see cref="RepositoryBase{T}"/>).
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class InMemoryRepository<T> : RepositoryBase<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the in-memory backend. Never null. The repository does not own or dispose it.
        /// </summary>
        public InMemoryBackend Store { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a repository.
        /// </summary>
        /// <param name="backend">Backend. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity needs a capability the backend does not advertise.</exception>
        public InMemoryRepository(InMemoryBackend backend, RepositoryOptions? options = null) : base(backend, options)
        {
            Store = backend;
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override bool AcceptsAmbientTransaction(ITransaction transaction)
        {
            return Store.Owns(transaction);
        }

        #endregion
    }
}
