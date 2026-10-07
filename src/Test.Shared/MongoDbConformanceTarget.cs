namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.MongoDb;

    /// <summary>
    /// Conformance target for the MongoDB backend: every repository shares the backend of a <see cref="MongoDbTestTarget"/>,
    /// and storage is reset by <see cref="MongoDbBackend.ClearAsync(Type, CancellationToken)"/> (drop the collection, reset its
    /// sequence, recreate it with its indexes). The capabilities are declared up front (the kit reads them before the
    /// server is contacted) and checked against the backend when it connects.
    /// </summary>
    public sealed class MongoDbConformanceTarget : IConformanceTarget
    {
        #region Public-Members

        /// <inheritdoc />
        public string Name => "MongoDB";

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities { get; }

        #endregion

        #region Private-Members

        private readonly Func<MongoDbBackend> _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a target.
        /// </summary>
        /// <param name="capabilities">Declared capabilities; must equal the backend's.</param>
        /// <param name="backend">Returns the connected backend. Must not be null.</param>
        public MongoDbConformanceTarget(RepositoryCapabilities capabilities, Func<MongoDbBackend> backend)
        {
            Capabilities = capabilities;
            _Backend = backend ?? throw new ArgumentNullException(nameof(backend));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IRepository<T> CreateRepository<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(RepositoryOptions? options = null) where T : class, new()
        {
            return _Backend().CreateRepository<T>(options);
        }

        /// <inheritdoc />
        public async Task ResetAsync(IReadOnlyList<Type> entityTypes, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            MongoDbBackend backend = _Backend();
            foreach (Type type in entityTypes)
            {
                token.ThrowIfCancellationRequested();
                await backend.ClearAsync(type, token).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
