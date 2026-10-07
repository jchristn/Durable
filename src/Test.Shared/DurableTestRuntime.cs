namespace Test.Shared
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Holds the shared repository provider and configuration used by every Touchstone test case.
    /// The provider's schema is created exactly once per process (idempotently) the first time a case runs,
    /// mirroring the single setup/teardown lifecycle used by the legacy shared test runner.
    /// Thread-safe: initialization is guarded by an async-aware lock.
    /// </summary>
    public static class DurableTestRuntime
    {
        #region Private-Members

        private static readonly object _SyncRoot = new object();
        private static readonly SemaphoreSlim _InitLock = new SemaphoreSlim(1, 1);
        private static TestRuntimeConfiguration _Configuration = LoadDefaultConfiguration();
        private static IRepositoryProvider? _Provider;
        private static bool _Initialized;
        private static IDocumentBackendTestTarget? _DocumentBackend;
        private static bool _DocumentBackendInitialized;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets a copy of the current runtime configuration.
        /// </summary>
        public static TestRuntimeConfiguration Configuration
        {
            get
            {
                lock (_SyncRoot)
                {
                    return _Configuration.Copy();
                }
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Replaces the runtime configuration. Must be called before any test case executes.
        /// </summary>
        /// <param name="configuration">The configuration to apply. Cannot be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when configuration changes after initialization.</exception>
        public static void Configure(TestRuntimeConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            lock (_SyncRoot)
            {
                if (_Initialized || _DocumentBackend != null)
                {
                    throw new InvalidOperationException("DurableTestRuntime cannot be reconfigured after initialization.");
                }

                _Configuration = configuration.Copy();
            }
        }

        /// <summary>
        /// Ensures the shared provider has been created and its schema initialized. Idempotent and thread-safe.
        /// </summary>
        /// <param name="token">A cancellation token.</param>
        /// <returns>The shared repository provider.</returns>
        public static async Task<IRepositoryProvider> EnsureInitializedAsync(CancellationToken token = default)
        {
            if (_Initialized && _Provider != null)
            {
                return _Provider;
            }

            await _InitLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (_Initialized && _Provider != null)
                {
                    return _Provider;
                }

                TestRuntimeConfiguration configuration;
                lock (_SyncRoot)
                {
                    configuration = _Configuration.Copy();
                }

                IRepositoryProvider provider = RepositoryProviderFactory.Create(configuration);
                await provider.SetupDatabaseAsync().ConfigureAwait(false);

                lock (_SyncRoot)
                {
                    _Provider = provider;
                    _Initialized = true;
                }

                return provider;
            }
            finally
            {
                _InitLock.Release();
            }
        }

        /// <summary>
        /// Returns the already-initialized shared provider. Intended for synchronous instance factories that run
        /// after <see cref="EnsureInitializedAsync"/> has completed (e.g., a Touchstone before-each hook).
        /// </summary>
        /// <returns>The initialized shared repository provider.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the provider has not been initialized yet.</exception>
        public static IRepositoryProvider RequireProvider()
        {
            lock (_SyncRoot)
            {
                if (_Provider == null)
                {
                    throw new InvalidOperationException("The shared provider has not been initialized. Ensure EnsureInitializedAsync has run first.");
                }

                return _Provider;
            }
        }

        /// <summary>
        /// Returns the shared document-backend target for the configured document backend (MongoDB, Cosmos DB), creating
        /// it on first use without connecting. Used while the suites are built; cases initialize it through
        /// <see cref="EnsureDocumentBackendInitializedAsync"/>. Thread-safe.
        /// </summary>
        /// <returns>The shared target. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the configured type is not a document backend.</exception>
        /// <exception cref="NotSupportedException">Thrown when the backend's test wiring has not been added yet.</exception>
        public static IDocumentBackendTestTarget GetDocumentBackend()
        {
            lock (_SyncRoot)
            {
                if (_DocumentBackend != null) return _DocumentBackend;
                if (!TestDatabaseTypes.IsDocumentBackend(_Configuration.DatabaseType))
                {
                    throw new InvalidOperationException(
                        TestDatabaseTypes.ProviderName(_Configuration.DatabaseType) + " is not a document backend; use EnsureInitializedAsync.");
                }

                _DocumentBackend = DocumentBackendTestTargets.Create(_Configuration.Copy());
                return _DocumentBackend;
            }
        }

        /// <summary>
        /// Ensures the shared document-backend target exists and has been initialized (connected, database created).
        /// Idempotent and thread-safe; the before-each hook of every document-backend case.
        /// </summary>
        /// <param name="token">A cancellation token.</param>
        /// <returns>The initialized shared target.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the configured type is not a document backend.</exception>
        public static async Task<IDocumentBackendTestTarget> EnsureDocumentBackendInitializedAsync(CancellationToken token = default)
        {
            IDocumentBackendTestTarget target = GetDocumentBackend();
            if (_DocumentBackendInitialized) return target;

            await _InitLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (!_DocumentBackendInitialized)
                {
                    await target.InitializeAsync(token).ConfigureAwait(false);
                    _DocumentBackendInitialized = true;
                }

                return target;
            }
            finally
            {
                _InitLock.Release();
            }
        }

        /// <summary>
        /// Cleans up the shared provider's schema and disposes it, and disposes the shared document-backend target when one
        /// was created. Safe to call multiple times.
        /// </summary>
        /// <returns>A task representing the asynchronous cleanup operation.</returns>
        public static async Task CleanupAsync()
        {
            IRepositoryProvider? provider;
            IDocumentBackendTestTarget? documentBackend;
            lock (_SyncRoot)
            {
                provider = _Provider;
                _Provider = null;
                _Initialized = false;
                documentBackend = _DocumentBackend;
                _DocumentBackend = null;
                _DocumentBackendInitialized = false;
            }

            if (documentBackend != null)
            {
                await documentBackend.DisposeAsync().ConfigureAwait(false);
            }

            if (provider == null)
            {
                return;
            }

            try
            {
                await provider.CleanupDatabaseAsync().ConfigureAwait(false);
            }
            finally
            {
                provider.Dispose();
            }
        }

        #endregion

        #region Private-Methods

        private static TestRuntimeConfiguration LoadDefaultConfiguration()
        {
            TestRuntimeConfiguration configuration = new TestRuntimeConfiguration();

            string? typeValue = Environment.GetEnvironmentVariable("DURABLE_TEST_DB");
            TestDatabaseType? parsed = TestDatabaseTypes.Parse(typeValue);
            if (parsed.HasValue)
            {
                configuration.DatabaseType = parsed.Value;
            }

            string? host = Environment.GetEnvironmentVariable("DURABLE_TEST_HOST");
            if (!string.IsNullOrWhiteSpace(host)) configuration.Hostname = host;

            string? port = Environment.GetEnvironmentVariable("DURABLE_TEST_PORT");
            if (!string.IsNullOrWhiteSpace(port) && int.TryParse(port, out int portValue)) configuration.Port = portValue;

            string? user = Environment.GetEnvironmentVariable("DURABLE_TEST_USER");
            if (!string.IsNullOrWhiteSpace(user)) configuration.Username = user;

            string? pass = Environment.GetEnvironmentVariable("DURABLE_TEST_PASS");
            if (!string.IsNullOrWhiteSpace(pass)) configuration.Password = pass;

            string? database = Environment.GetEnvironmentVariable("DURABLE_TEST_DATABASE");
            if (!string.IsNullOrWhiteSpace(database)) configuration.DatabaseName = database;

            string? filename = Environment.GetEnvironmentVariable("DURABLE_TEST_FILE");
            if (!string.IsNullOrWhiteSpace(filename)) configuration.Filename = filename;

            return configuration;
        }

        #endregion
    }
}
