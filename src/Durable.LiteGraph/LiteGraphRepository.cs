namespace Durable.LiteGraph
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// A repository stored in a LiteGraph graph through a <see cref="LiteGraphBackend"/>: the complete
    /// <see cref="IRepository{T}"/> surface from <see cref="RepositoryBase{T}"/>, with each entity row stored as a node
    /// labelled with the entity's table name and foreign keys maintained as edges. Repositories created over the same
    /// backend share the graph, so includes, navigation predicates and transactions work across entity types. An ambient
    /// <see cref="AmbientTransactionScope"/> is used only when its transaction was created by the same backend. The repository
    /// never disposes the backend.
    /// Thread safety: safe for concurrent use once configured (see <see cref="RepositoryBase{T}"/>).
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class LiteGraphRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : RepositoryBase<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the LiteGraph backend. Never null. The repository does not own or dispose it.
        /// </summary>
        public new LiteGraphBackend Backend { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a repository over a backend shared with other repositories. Equivalent to
        /// <see cref="LiteGraphBackend.CreateRepository{T}"/>.
        /// </summary>
        /// <param name="backend">Backend. Must not be null. Not disposed by the repository.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public LiteGraphRepository(LiteGraphBackend backend, RepositoryOptions? options = null) : base(backend, options)
        {
            Backend = backend;
            Backend.Register(Metadata);
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override bool AcceptsAmbientTransaction(ITransaction transaction)
        {
            return Backend.Owns(transaction);
        }

        #endregion
    }
}
