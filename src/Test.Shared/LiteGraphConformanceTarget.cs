namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.LiteGraph;

    /// <summary>
    /// Runs the Durable.Conformance kit against the LiteGraph backend, stored in a temporary SQLite file (LiteGraph's
    /// durable storage mode) that is created on first use and deleted when the process exits. Resetting storage deletes
    /// every node of each type (and its edges) directly through the backend, bypassing soft delete; other types are
    /// untouched.
    /// Thread safety: safe for concurrent use; the backend is thread-safe.
    /// </summary>
    public sealed class LiteGraphConformanceTarget : IConformanceTarget
    {
        #region Public-Members

        /// <inheritdoc />
        public string Name => "LiteGraph";

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => RepositoryCapabilities.All;

        #endregion

        #region Private-Members

        private readonly Lazy<LiteGraphBackend> _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the target; the backend and its database file are created on first use.
        /// </summary>
        public LiteGraphConformanceTarget()
        {
            _Backend = new Lazy<LiteGraphBackend>(CreateBackend, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IRepository<T> CreateRepository<T>(RepositoryOptions? options = null) where T : class, new()
        {
            return _Backend.Value.CreateRepository<T>(options);
        }

        /// <inheritdoc />
        public async Task ResetAsync(IReadOnlyList<Type> entityTypes, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            foreach (Type type in entityTypes)
            {
                token.ThrowIfCancellationRequested();
                await _Backend.Value.ClearAsync(type, token).ConfigureAwait(false);
            }
        }

        #endregion

        #region Private-Methods

        private static LiteGraphBackend CreateBackend()
        {
            string directory = Path.Combine(Path.GetTempPath(), "durable-litegraph-conformance-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            LiteGraphBackend backend = LiteGraphBackend.Create(LiteGraphRepositorySettings.ForFile(Path.Combine(directory, "conformance.db")));
            AppDomain.CurrentDomain.ProcessExit += (sender, args) =>
            {
                backend.Dispose();
                try
                {
                    Directory.Delete(directory, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            };

            return backend;
        }

        #endregion
    }
}
