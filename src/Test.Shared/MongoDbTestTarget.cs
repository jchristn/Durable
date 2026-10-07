namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.MongoDb;
    using MongoDB.Bson;
    using MongoDB.Driver;
    using Touchstone.Core;

    /// <summary>
    /// Test wiring of the MongoDB backend (<c>--type mongodb</c>): one shared <see cref="MongoDbBackend"/> over the configured
    /// server (host, port, user, password; the database is <see cref="TestRuntimeConfiguration.DatabaseName"/>, cleared when
    /// the target connects), the Durable.Conformance kit through <see cref="MongoDbConformanceTarget"/>, and
    /// <see cref="MongoDbBackendTestSuite"/>. Connections are direct (no replica set discovery), so a single-node replica set
    /// in a container works from the host. The declared capabilities include transactions (a replica set, as the docker run
    /// starts); set the environment variable <c>DURABLE_TEST_MONGODB_STANDALONE=1</c> when testing against a standalone server,
    /// so transactions are declared unsupported and their cases are gated.
    /// </summary>
    public sealed class MongoDbTestTarget : IDocumentBackendTestTarget
    {
        #region Public-Members

        /// <inheritdoc />
        public TestDatabaseType DatabaseType => TestDatabaseType.MongoDb;

        /// <inheritdoc />
        public IConformanceTarget ConformanceTarget { get; }

        /// <summary>
        /// Gets the declared capabilities (all, without transactions for a standalone server).
        /// </summary>
        public RepositoryCapabilities DeclaredCapabilities { get; }

        /// <summary>
        /// Gets the shared backend. Throws before <see cref="InitializeAsync"/> has completed.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown before initialization.</exception>
        public MongoDbBackend Backend => _Backend ?? throw new InvalidOperationException("The MongoDB test target has not been initialized.");

        #endregion

        #region Private-Members

        private readonly TestRuntimeConfiguration _Configuration;
        private readonly SemaphoreSlim _Lock = new SemaphoreSlim(1, 1);
        private MongoDbBackend? _Backend;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the target without connecting.
        /// </summary>
        /// <param name="configuration">Run configuration. Must not be null.</param>
        public MongoDbTestTarget(TestRuntimeConfiguration configuration)
        {
            _Configuration = configuration?.Copy() ?? throw new ArgumentNullException(nameof(configuration));
            bool standalone = string.Equals(Environment.GetEnvironmentVariable("DURABLE_TEST_MONGODB_STANDALONE"), "1", StringComparison.Ordinal);
            DeclaredCapabilities = standalone ? RepositoryCapabilities.All & ~RepositoryCapabilities.Transactions : RepositoryCapabilities.All;
            ConformanceTarget = new MongoDbConformanceTarget(DeclaredCapabilities, () => Backend);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds settings for the configured server and a database (default: the configured database).
        /// </summary>
        /// <param name="databaseName">Database name; null for the configured one.</param>
        /// <returns>New settings. Never null.</returns>
        public MongoDbRepositorySettings CreateSettings(string? databaseName = null)
        {
            return CreateSettings(_Configuration, databaseName);
        }

        /// <summary>
        /// Builds settings for a configuration's server.
        /// </summary>
        /// <param name="configuration">Configuration. Must not be null.</param>
        /// <param name="databaseName">Database name; null for the configured one.</param>
        /// <returns>New settings. Never null.</returns>
        public static MongoDbRepositorySettings CreateSettings(TestRuntimeConfiguration configuration, string? databaseName = null)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            MongoDbRepositorySettings settings = new MongoDbRepositorySettings
            {
                Hostname = string.IsNullOrWhiteSpace(configuration.Hostname) ? "localhost" : configuration.Hostname,
                Port = configuration.Port ?? 27017,
                DatabaseName = databaseName ?? (string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName),
                DirectConnection = true,
                ServerSelectionTimeout = TimeSpan.FromSeconds(10),
                ApplicationName = "durable-touchstone"
            };

            if (!string.IsNullOrWhiteSpace(configuration.Username))
            {
                settings.Username = configuration.Username;
                settings.Password = configuration.Password;
            }

            return settings;
        }

        /// <summary>
        /// Initiates a single-node replica set on the configured server if the server was started with <c>--replSet</c> and
        /// is not initiated yet, then waits until it is a writable primary. Used by the docker readiness probe. Throws while
        /// the server is not reachable or not yet primary (the probe retries).
        /// </summary>
        /// <param name="configuration">Configuration. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the server is a writable primary.</returns>
        public static async Task InitiateReplicaSetAsync(TestRuntimeConfiguration configuration, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(configuration);
            MongoDbRepositorySettings settings = CreateSettings(configuration);
            settings.ServerSelectionTimeout = TimeSpan.FromSeconds(3);
            MongoClientSettings clientSettings = settings.ToClientSettings();
            using MongoClient client = new MongoClient(clientSettings);
            IMongoDatabase admin = client.GetDatabase("admin");
            BsonDocument hello = await admin.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1), null, token).ConfigureAwait(false);
            if (hello.GetValue("isWritablePrimary", false).ToBoolean()) return;

            try
            {
                await admin.RunCommandAsync<BsonDocument>(new BsonDocument("replSetGetStatus", 1), null, token).ConfigureAwait(false);
            }
            catch (MongoCommandException e) when (e.Code == 94 || e.CodeName == "NotYetInitialized")
            {
                BsonDocument config = new BsonDocument
                {
                    { "_id", "rs0" },
                    { "members", new BsonArray { new BsonDocument { { "_id", 0 }, { "host", "localhost:27017" } } } }
                };
                await admin.RunCommandAsync<BsonDocument>(new BsonDocument("replSetInitiate", config), null, token).ConfigureAwait(false);
            }

            throw new InvalidOperationException("MongoDB is not a writable primary yet.");
        }

        /// <inheritdoc />
        public async Task InitializeAsync(CancellationToken token = default)
        {
            if (_Backend != null) return;
            await _Lock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (_Backend != null) return;
                MongoDbBackend backend = await MongoDbBackend.CreateAsync(CreateSettings(), token).ConfigureAwait(false);
                try
                {
                    if (backend.Capabilities != DeclaredCapabilities)
                    {
                        throw new InvalidOperationException(
                            "The MongoDB server's capabilities (" + backend.Capabilities + ") differ from the declared ones (" + DeclaredCapabilities + "). "
                            + "Run a replica set (the docker run does), or set DURABLE_TEST_MONGODB_STANDALONE=1 for a standalone server.");
                    }

                    await backend.ClearAsync(token).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await backend.DisposeAsync().ConfigureAwait(false);
                    throw;
                }

                _Backend = backend;
            }
            finally
            {
                _Lock.Release();
            }
        }

        /// <inheritdoc />
        public IReadOnlyList<TestSuiteDescriptor> BuildBackendSuites(IReadOnlyList<string> tags, Func<CancellationToken, Task> beforeEach)
        {
            List<string> suiteTags = new List<string>(tags) { "mongodb" };
            return new List<TestSuiteDescriptor>
            {
                TouchstoneBridge.BuildSuite<MongoDbBackendTestSuite>(
                    "MongoDb.Backend",
                    "MongoDB Backend (Persistence / Push-down / Precision / Indexes / Concurrency / Transactions / Ownership) Tests",
                    () => new MongoDbBackendTestSuite(this),
                    suiteTags,
                    beforeEach)
            };
        }

        /// <summary>
        /// Disposes the shared backend.
        /// </summary>
        /// <returns>A task that completes when the backend is disposed.</returns>
        public async ValueTask DisposeAsync()
        {
            MongoDbBackend? backend = _Backend;
            _Backend = null;
            if (backend != null) await backend.DisposeAsync().ConfigureAwait(false);
        }

        #endregion
    }
}
