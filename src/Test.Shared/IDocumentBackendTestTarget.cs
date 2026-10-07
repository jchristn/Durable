namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Conformance;
    using Touchstone.Core;

    /// <summary>
    /// The test wiring of a non-SQL document backend (MongoDB, Cosmos DB). When <see cref="TestRuntimeConfiguration.DatabaseType"/>
    /// is a document backend (see <see cref="TestDatabaseTypes.IsDocumentBackend"/>), <see cref="DurableTestSuites.All"/>
    /// builds no <see cref="IRepositoryProvider"/> and none of the SQL suites; it runs the Durable.Conformance kit against
    /// <see cref="ConformanceTarget"/> and the suites returned by <see cref="BuildBackendSuites"/>, plus the backend-neutral
    /// unit suites every configuration runs.
    /// <para>
    /// Lifecycle: <see cref="DocumentBackendTestTargets.Create"/> constructs the target from the run's configuration
    /// (host, port, user, password, database name) while the suites are being built, so the constructor must not connect.
    /// <see cref="InitializeAsync"/> runs once, before the first case (and by the docker runner's default readiness probe),
    /// and opens the connection and creates the database. <see cref="DurableTestRuntime.CleanupAsync"/> disposes the target
    /// at the end of the run.
    /// </para>
    /// Thread safety: Touchstone runs cases sequentially; implementations need not be thread-safe beyond that.
    /// </summary>
    public interface IDocumentBackendTestTarget : IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the backend served by this target (<see cref="TestDatabaseType.MongoDb"/> or <see cref="TestDatabaseType.CosmosDb"/>).
        /// </summary>
        TestDatabaseType DatabaseType { get; }

        /// <summary>
        /// Gets the conformance target passed to <see cref="ConformanceSuites.Build"/>. Available right after construction:
        /// its <see cref="IConformanceTarget.Capabilities"/> are read when the suites are built, before
        /// <see cref="InitializeAsync"/> has run, so they must not require a connection. Never null.
        /// </summary>
        IConformanceTarget ConformanceTarget { get; }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Connects to the backend and prepares the test database. Called before every case; must be idempotent and cheap
        /// after the first successful call. Throws while the server is not reachable yet (the docker runner retries it).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the backend is ready.</returns>
        Task InitializeAsync(CancellationToken token = default);

        /// <summary>
        /// Builds the backend's own suites (persistence, push-down, settings, ...), in addition to the conformance kit.
        /// </summary>
        /// <param name="tags">Tags to apply to every case (the provider tag plus the backend tag). Never null.</param>
        /// <param name="beforeEach">Delegate to await before every case; it initializes this target. Never null.</param>
        /// <returns>The suites; empty when the backend has none. Never null.</returns>
        IReadOnlyList<TestSuiteDescriptor> BuildBackendSuites(IReadOnlyList<string> tags, Func<CancellationToken, Task> beforeEach);

        #endregion
    }
}
