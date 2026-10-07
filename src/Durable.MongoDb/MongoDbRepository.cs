namespace Durable.MongoDb
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// A repository stored in MongoDB: the complete <see cref="IRepository{T}"/> surface from <see cref="RepositoryBase{T}"/>
    /// over a <see cref="MongoDbBackend"/>. Repositories created over the same backend share data, so includes, navigation
    /// predicates and transactions work across entity types. An ambient <see cref="AmbientTransactionScope"/> is used only
    /// when its transaction was created by the same backend. Creating a repository validates the entity's MongoDB mapping;
    /// its indexes are created on first use (see <see cref="MongoDbBackend.EnsureIndexesAsync"/>). The repository never
    /// disposes the backend.
    /// <para>
    /// Entities that cannot be stored (reported with <see cref="NotSupportedException"/> by the constructor): a table name
    /// that is not a MongoDB collection name (empty, containing '$' or a null character, or starting with "system." or '.'),
    /// a column name that is not a MongoDB field name (empty, containing '.' or a null character, or starting with '$'), a
    /// column named <c>_id</c> that is not the single primary key, and an auto-increment column that is not a single
    /// integer primary key.
    /// </para>
    /// Thread safety: safe for concurrent use once configured (see <see cref="RepositoryBase{T}"/>).
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class MongoDbRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : RepositoryBase<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the MongoDB backend. Never null. The repository does not own or dispose it.
        /// </summary>
        public new MongoDbBackend Backend { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a repository over a backend shared with other repositories. Equivalent to
        /// <see cref="MongoDbBackend.CreateRepository{T}"/>. Does not contact the server.
        /// </summary>
        /// <param name="backend">Backend. Must not be null. Not disposed by the repository.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public MongoDbRepository(MongoDbBackend backend, RepositoryOptions? options = null) : base(backend, options)
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
