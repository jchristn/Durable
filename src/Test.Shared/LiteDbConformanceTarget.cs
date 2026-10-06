namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.LiteDb;
    using Durable.Query;

    /// <summary>
    /// Conformance target for the LiteDB backend over an in-memory LiteDB database: every repository shares one
    /// <see cref="LiteDbBackend"/>, and storage is reset by deleting the documents of the requested entity types.
    /// </summary>
    public sealed class LiteDbConformanceTarget : IConformanceTarget, IDisposable
    {
        #region Public-Members

        /// <inheritdoc />
        public string Name => "LiteDB";

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => _Backend.Capabilities;

        #endregion

        #region Private-Members

        private readonly LiteDbBackend _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a target over a new in-memory LiteDB database.
        /// </summary>
        public LiteDbConformanceTarget()
        {
            _Backend = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory());
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IRepository<T> CreateRepository<T>(RepositoryOptions? options = null) where T : class, new()
        {
            return _Backend.CreateRepository<T>(options);
        }

        /// <inheritdoc />
        public async Task ResetAsync(IReadOnlyList<Type> entityTypes, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            foreach (Type type in entityTypes)
            {
                token.ThrowIfCancellationRequested();
                await _Backend.DeleteAsync(new QueryModel(new QuerySource(EntityMetadata.For(type))), token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Disposes the backend and its in-memory database.
        /// </summary>
        public void Dispose()
        {
            _Backend.Dispose();
        }

        #endregion
    }
}
