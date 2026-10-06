namespace Durable.LiteDb
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Durable;
    using Durable.Query;
    using LiteDB;

    /// <summary>
    /// A repository stored in LiteDB: the complete <see cref="IRepository{T}"/> surface from <see cref="RepositoryBase{T}"/>
    /// over a <see cref="LiteDbBackend"/>. Repositories created over the same backend (or the same
    /// <see cref="LiteDatabase"/>) share data, so includes, navigation predicates and transactions work across entity types.
    /// An ambient <see cref="TransactionScope"/> is used only when its transaction was created over the same database.
    /// Creating a repository validates the entity's LiteDB mapping and creates its indexes.
    /// <para>
    /// Entities that cannot be stored (reported with <see cref="NotSupportedException"/> by the constructor): a table name
    /// that is not a LiteDB collection name (letters, digits, '_' and '$', not starting with a digit or '$'), two columns
    /// whose names differ only in case (LiteDB field names are case-insensitive), a column named <c>_id</c> that is not the
    /// single primary key, and an auto-increment column that is not a single integer primary key.
    /// </para>
    /// Thread safety: safe for concurrent use once configured (see <see cref="RepositoryBase{T}"/>).
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class LiteDbRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : RepositoryBase<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the LiteDB backend. Never null. The repository does not own or dispose it.
        /// </summary>
        public LiteDbBackend Store { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a repository over a backend shared with other repositories.
        /// </summary>
        /// <param name="backend">Backend. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the backend has been disposed.</exception>
        public LiteDbRepository(LiteDbBackend backend, RepositoryOptions? options = null) : base(backend, options)
        {
            Store = backend;
            Store.EnsureIndexes(typeof(T));
        }

        /// <summary>
        /// Instantiates a repository over an existing LiteDB database, which it does not dispose. Prefer one
        /// <see cref="LiteDbBackend"/> shared by all repositories; repositories created over the same database with this
        /// constructor still share data and transactions.
        /// </summary>
        /// <param name="database">Database. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when database is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in LiteDB.</exception>
        public LiteDbRepository(LiteDatabase database, RepositoryOptions? options = null) : this(new LiteDbBackend(database), options)
        {
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
