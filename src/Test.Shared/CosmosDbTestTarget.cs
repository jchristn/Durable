namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Conformance;
    using Durable.CosmosDb;
    using Touchstone.Core;

    /// <summary>
    /// Test wiring of the Cosmos DB backend (<c>--type cosmosdb</c>): connects to the account or emulator named by the run's
    /// configuration (host and port, or a full endpoint URI as the host; the password is the account key and defaults to
    /// the emulator's well-known key), runs the conformance kit through <see cref="CosmosDbConformanceTarget"/> and adds
    /// <see cref="CosmosDbBackendTestSuite"/>. Owned clients use gateway mode with endpoint discovery disabled, which the
    /// emulator and mapped container ports require.
    /// </summary>
    public sealed class CosmosDbTestTarget : IDocumentBackendTestTarget
    {
        #region Public-Members

        /// <inheritdoc />
        public TestDatabaseType DatabaseType => TestDatabaseType.CosmosDb;

        /// <inheritdoc />
        public IConformanceTarget ConformanceTarget { get; }

        /// <summary>
        /// Gets the account endpoint the target connects to. Never null.
        /// </summary>
        public string Endpoint { get; }

        /// <summary>
        /// Gets the database name used by the target. Never null.
        /// </summary>
        public string DatabaseName { get; }

        #endregion

        #region Private-Members

        private readonly string _AccountKey;
        private CosmosDbBackend? _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the target from the run's configuration without connecting.
        /// </summary>
        /// <param name="configuration">Configuration. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
        public CosmosDbTestTarget(TestRuntimeConfiguration configuration)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            string host = string.IsNullOrWhiteSpace(configuration.Hostname) ? "localhost" : configuration.Hostname;
            if (host.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || host.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                Endpoint = host.EndsWith("/", StringComparison.Ordinal) ? host : host + "/";
            }
            else
            {
                int port = configuration.Port ?? 8081;
                Endpoint = "http://" + host + ":" + port.ToString(CultureInfo.InvariantCulture) + "/";
            }

            _AccountKey = string.IsNullOrWhiteSpace(configuration.Password) ? CosmosDbRepositorySettings.EmulatorAccountKey : configuration.Password;
            DatabaseName = string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName;
            ConformanceTarget = new CosmosDbConformanceTarget(() => Backend);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Gets the shared backend. Available after <see cref="InitializeAsync"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown before initialization.</exception>
        public CosmosDbBackend Backend => _Backend ?? throw new InvalidOperationException("The Cosmos DB test target has not been initialized.");

        /// <summary>
        /// Creates settings for a new owned client on the target's account and database.
        /// </summary>
        /// <returns>New settings. Never null.</returns>
        public CosmosDbRepositorySettings CreateSettings()
        {
            CosmosDbRepositorySettings settings = CosmosDbRepositorySettings.ForEmulator(Endpoint, DatabaseName);
            settings.AccountKey = _AccountKey;
            settings.RequestTimeout = TimeSpan.FromSeconds(30);
            return settings;
        }

        /// <inheritdoc />
        public async Task InitializeAsync(CancellationToken token = default)
        {
            if (_Backend != null) return;
            _Backend = await CosmosDbBackend.CreateAsync(CreateSettings(), token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public IReadOnlyList<TestSuiteDescriptor> BuildBackendSuites(IReadOnlyList<string> tags, Func<CancellationToken, Task> beforeEach)
        {
            ArgumentNullException.ThrowIfNull(tags);
            ArgumentNullException.ThrowIfNull(beforeEach);
            List<string> suiteTags = new List<string>(tags) { "cosmosdb" };
            return new List<TestSuiteDescriptor>
            {
                TouchstoneBridge.BuildSuite<CosmosDbBackendTestSuite>(
                    "CosmosDb.Backend",
                    "Cosmos DB Backend (Persistence / Push-down / Precision / Concurrency / Partition Keys / Ownership) Tests",
                    () => new CosmosDbBackendTestSuite(this),
                    suiteTags,
                    beforeEach)
            };
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            CosmosDbBackend? backend = _Backend;
            _Backend = null;
            return backend == null ? ValueTask.CompletedTask : backend.DisposeAsync();
        }

        #endregion
    }
}
