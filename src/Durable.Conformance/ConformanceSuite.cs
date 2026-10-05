namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Base class for conformance suites. The kit creates a fresh instance for every case, sets <see cref="Token"/>,
    /// invokes one <see cref="ConformanceTestAttribute"/> method and disposes the instance, which disposes every repository
    /// created through <see cref="Repository{T}"/>. Derive from it (with a public constructor taking an
    /// <see cref="IConformanceTarget"/>) to add backend-specific suites with <see cref="ConformanceSuites.BuildSuite"/>.
    /// Thread safety: an instance serves one case on one flow and is not thread-safe.
    /// </summary>
    public abstract class ConformanceSuite : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the backend under test. Never null.
        /// </summary>
        public IConformanceTarget Target { get; }

        /// <summary>
        /// Gets or sets the cancellation token of the running case. Default: <see cref="CancellationToken.None"/>.
        /// </summary>
        public CancellationToken Token { get; set; } = CancellationToken.None;

        #endregion

        #region Private-Members

        private readonly List<IDisposable> _Owned = new List<IDisposable>();
        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the suite for a target.
        /// </summary>
        /// <param name="target">Backend under test. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when target is null.</exception>
        protected ConformanceSuite(IConformanceTarget target)
        {
            Target = target ?? throw new ArgumentNullException(nameof(target));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes every repository created through <see cref="Repository{T}"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Protected-Methods

        /// <summary>
        /// Determines whether the target supports all of the given capabilities.
        /// </summary>
        /// <param name="capabilities">Capabilities to check.</param>
        /// <returns>True when every flag is supported.</returns>
        protected bool Supports(RepositoryCapabilities capabilities)
        {
            return (Target.Capabilities & capabilities) == capabilities;
        }

        /// <summary>
        /// Resets the storage of the given entity types through <see cref="IConformanceTarget.ResetAsync"/>.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        protected Task ResetAsync(params Type[] entityTypes)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            return Target.ResetAsync(entityTypes, Token);
        }

        /// <summary>
        /// Creates a repository through the target and disposes it with the suite.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="options">Options; null for defaults.</param>
        /// <returns>The repository.</returns>
        protected IRepository<T> Repository<T>(RepositoryOptions? options = null) where T : class, new()
        {
            IRepository<T> repository = Target.CreateRepository<T>(options);
            if (repository == null) throw new InvalidOperationException("Target " + Target.Name + " returned a null repository for " + typeof(T).Name + ".");
            _Owned.Add(repository);
            return repository;
        }

        /// <summary>
        /// Releases resources.
        /// </summary>
        /// <param name="disposing">True when called from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed) return;
            if (disposing)
            {
                for (int i = _Owned.Count - 1; i >= 0; i--) _Owned[i].Dispose();
                _Owned.Clear();
            }

            _Disposed = true;
        }

        #endregion
    }
}
