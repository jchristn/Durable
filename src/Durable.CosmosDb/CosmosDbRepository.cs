namespace Durable.CosmosDb
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// A repository stored in Azure Cosmos DB for NoSQL: the complete <see cref="IRepository{T}"/> surface from
    /// <see cref="RepositoryBase{T}"/> over a <see cref="CosmosDbBackend"/>, except transactions (see
    /// <see cref="CosmosDbBackend"/>). Repositories created over the same backend (or over backends using the same account
    /// and database) share data, so includes and navigation predicates work across entity types. Creating a repository
    /// validates the entity's Cosmos DB mapping but does not touch the account; the container is created on first use. The
    /// repository never disposes the backend.
    /// <para>
    /// Entities that cannot be stored (reported with <see cref="NotSupportedException"/> by the constructor): a table name
    /// that is not a Cosmos DB container name (1 to 255 characters, no '/', '\', '#' or '?', no trailing space), two
    /// columns mapping to the same document property, a value type JSON cannot hold without a value converter, a partition
    /// key that is not a mapped string, integer, Guid, date or boolean column, and an auto-increment column that is not an
    /// integer.
    /// </para>
    /// Thread safety: safe for concurrent use once configured (see <see cref="RepositoryBase{T}"/>).
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class CosmosDbRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : RepositoryBase<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the Cosmos DB backend. Never null. The repository does not own or dispose it.
        /// </summary>
        public new CosmosDbBackend Backend { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a repository over a backend shared with other repositories. Equivalent to
        /// <see cref="CosmosDbBackend.CreateRepository{T}"/>.
        /// </summary>
        /// <param name="backend">Backend. Must not be null. Not disposed by the repository.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public CosmosDbRepository(CosmosDbBackend backend, RepositoryOptions? options = null) : base(backend, options)
        {
            Backend = backend;
            Backend.ValidateMapping(typeof(T));
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
