namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.CosmosDb;

    /// <summary>
    /// Conformance target for the Cosmos DB backend: every repository shares the test target's <see cref="CosmosDbBackend"/>
    /// (one database on the emulator or account), and storage is reset by deleting the documents of the requested entity
    /// types (which also restarts their auto-increment counters). Capabilities are the backend's static
    /// <see cref="CosmosDbBackend.SupportedCapabilities"/>, so they are known before the backend connects.
    /// </summary>
    public sealed class CosmosDbConformanceTarget : IConformanceTarget
    {
        #region Public-Members

        /// <inheritdoc />
        public string Name => "Cosmos DB";

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => CosmosDbBackend.SupportedCapabilities;

        #endregion

        #region Private-Members

        private readonly Func<CosmosDbBackend> _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a target over a backend supplied once it is connected.
        /// </summary>
        /// <param name="backend">Returns the connected backend. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        public CosmosDbConformanceTarget(Func<CosmosDbBackend> backend)
        {
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
            CosmosDbBackend backend = _Backend();
            foreach (Type type in entityTypes)
            {
                token.ThrowIfCancellationRequested();
                await backend.ClearAsync(type, token).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
