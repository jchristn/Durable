namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.InMemory;
    using Durable.Query;

    /// <summary>
    /// Runs the Durable.Conformance kit against the in-memory backend, the reference non-SQL implementation built on
    /// <see cref="RepositoryBase{T}"/>. Resetting storage deletes every stored row of each type directly through the
    /// backend (bypassing soft delete); other types are untouched.
    /// Thread safety: safe for concurrent use; the backend is thread-safe.
    /// </summary>
    public sealed class InMemoryConformanceTarget : IConformanceTarget
    {
        #region Public-Members

        /// <inheritdoc />
        public string Name => "In-Memory";

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => _Backend.Capabilities;

        #endregion

        #region Private-Members

        private readonly InMemoryBackend _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the target over a new backend.
        /// </summary>
        /// <param name="capabilities">Capabilities the backend advertises. Default: all.</param>
        public InMemoryConformanceTarget(RepositoryCapabilities capabilities = RepositoryCapabilities.All)
        {
            _Backend = new InMemoryBackend(capabilities);
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

        #endregion
    }
}
