namespace Test.Shared
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Registry of the document-backend test targets, keyed by <see cref="TestDatabaseType"/>. Each document backend
    /// registers one line in <see cref="Create"/>; everything else (its <see cref="IDocumentBackendTestTarget"/>, its
    /// conformance target and its suites) lives in its own files.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public static class DocumentBackendTestTargets
    {
        #region Public-Methods

        /// <summary>
        /// Creates the test target for the configured document backend. Does not connect (see
        /// <see cref="IDocumentBackendTestTarget.InitializeAsync"/>).
        /// </summary>
        /// <param name="configuration">The run's configuration (host, port, user, password, database name). Must not be null.</param>
        /// <returns>A new target the caller disposes. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the configured type is not a document backend.</exception>
        /// <exception cref="NotSupportedException">Thrown when the backend's test wiring has not been added yet.</exception>
        public static IDocumentBackendTestTarget Create(TestRuntimeConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);

            switch (configuration.DatabaseType)
            {
                case TestDatabaseType.MongoDb:
                    return new MongoDbTestTarget(configuration);

                case TestDatabaseType.CosmosDb:
                    throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.CosmosDb, "document backend test target");

                default:
                    throw new ArgumentOutOfRangeException(
                        nameof(configuration),
                        TestDatabaseTypes.ProviderName(configuration.DatabaseType) + " is not a document backend; it runs through RepositoryProviderFactory.");
            }
        }

        /// <summary>
        /// Readiness probe for a document backend: creates a target for the configuration, initializes it and disposes it.
        /// Throws while the backend is not ready. The docker runner uses it when the backend's docker settings supply no
        /// probe of their own.
        /// </summary>
        /// <param name="configuration">The configuration to probe. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the backend accepted a connection.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration"/> is null.</exception>
        public static async Task ProbeAsync(TestRuntimeConfiguration configuration, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            IDocumentBackendTestTarget target = Create(configuration);
            await using (target.ConfigureAwait(false))
            {
                await target.InitializeAsync(token).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
