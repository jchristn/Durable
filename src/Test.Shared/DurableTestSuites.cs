namespace Test.Shared
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Touchstone.Core;

    /// <summary>
    /// The single source of truth for the Durable test suites. Produces runner-agnostic Touchstone
    /// <see cref="TestSuiteDescriptor"/> instances that are consumed identically by the CLI runner
    /// (Test.Automated), the xUnit adapter (Test.Xunit), and the NUnit adapter (Test.Nunit).
    ///
    /// Provider-agnostic behavioral suites run against whichever provider is configured on
    /// <see cref="DurableTestRuntime"/> (SQLite by default). Provider-specific unit suites run only for
    /// their owning provider. Document backends (<see cref="TestDatabaseTypes.IsDocumentBackend"/>) replace the
    /// SQL suites with the conformance kit and the suites of their <see cref="IDocumentBackendTestTarget"/>.
    /// </summary>
    public static class DurableTestSuites
    {
        #region Public-Members

        /// <summary>
        /// Gets all test suites for the currently configured provider.
        /// </summary>
        public static IReadOnlyList<TestSuiteDescriptor> All
        {
            get
            {
                TestRuntimeConfiguration configuration = DurableTestRuntime.Configuration;
                string providerTag = TestDatabaseTypes.ProviderTag(configuration.DatabaseType);

                // Document backends (MongoDB, Cosmos DB) build no IRepositoryProvider: they run the conformance kit and
                // their own suites through an IDocumentBackendTestTarget instead of the SQL suites, plus the
                // backend-neutral suites every configuration runs.
                bool documentBackend = TestDatabaseTypes.IsDocumentBackend(configuration.DatabaseType);

                Task BeforeEach(CancellationToken token) => DurableTestRuntime.EnsureInitializedAsync(token);

                List<TestSuiteDescriptor> suites = new List<TestSuiteDescriptor>();

                if (documentBackend)
                {
                    AddDocumentBackendSuites(suites, providerTag);
                }
                else
                {
                    AddSqlProviderSuites(suites, providerTag, BeforeEach);
                }

                // Backend-neutral unit suites (no database).
                suites.Add(TouchstoneBridge.BuildSuite<QueryNormalizerTestSuite>(
                    "QueryNormalizer", "Query Normalizer (Neutral Query Model) Tests", () => new QueryNormalizerTestSuite(), new List<string> { providerTag, "neutral" }));
                suites.Add(TouchstoneBridge.BuildSuite<QueryEvaluatorTestSuite>(
                    "QueryEvaluator", "Query Evaluator (Client-Side Node Evaluation) Tests", () => new QueryEvaluatorTestSuite(), new List<string> { providerTag, "neutral" }));
                suites.Add(TouchstoneBridge.BuildSuite<PublicApiConventionsTestSuite>(
                    "PublicApiConventions", "Public API Conventions (Tokens / Async Suffix / Sync-Async Pairs / Tuples / Out-Ref) Tests", () => new PublicApiConventionsTestSuite(), new List<string> { providerTag, "neutral" }));
                suites.Add(TouchstoneBridge.BuildSuite<ResolverAndDefaultValueTestSuite>(
                    "ResolverDefaultValue", "Conflict Resolver / Default Value Provider (Direct) Tests", () => new ResolverAndDefaultValueTestSuite(), new List<string> { providerTag, "neutral" }));
                suites.Add(TouchstoneBridge.BuildSuite<ProviderSettingsTestSuite>(
                    "ProviderSettings", "Provider Settings / Connection Factory Mapping Tests", () => new ProviderSettingsTestSuite(), new List<string> { providerTag, "neutral" }));

                // Durable.Conformance kit: the backend-neutral suites every IRepository<T> backend must pass, run here
                // against the configured SQL provider (SqlConformanceTarget resets storage by dropping and recreating tables).
                if (!documentBackend)
                {
                    // Oracle stores an empty string as NULL (OracleDialect.TreatsEmptyStringAsNull), so its repositories lack
                    // RepositoryCapabilities.EmptyStrings and the kit skips the cases that need it, naming the capability.
                    RepositoryCapabilities sqlCapabilities = configuration.DatabaseType == TestDatabaseType.Oracle
                        ? RepositoryCapabilities.All & ~RepositoryCapabilities.EmptyStrings
                        : RepositoryCapabilities.All;
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(
                        new SqlConformanceTarget(TestDatabaseTypes.ProviderName(configuration.DatabaseType), DurableTestRuntime.RequireProvider, sqlCapabilities),
                        "Conformance",
                        new List<string> { providerTag, "conformance" },
                        BeforeEach))
                    {
                        suites.Add(conformance);
                    }
                }

                // The same kit against the in-memory backend (no database), once in the default SQLite configuration, and
                // against a deliberately limited in-memory backend to prove unsupported calls fail at the call site.
                if (configuration.DatabaseType == TestDatabaseType.Sqlite)
                {
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(new InMemoryConformanceTarget(), "Conformance.InMemory", new List<string> { providerTag, "conformance", "inmemory" }))
                        suites.Add(conformance);
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(new InMemoryConformanceTarget(RepositoryCapabilities.None), "Conformance.InMemoryMinimal", new List<string> { providerTag, "conformance", "inmemory" }))
                        suites.Add(conformance);

                    // The same kit against the LiteDB backend (in-memory LiteDB database).
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(new LiteDbConformanceTarget(), "Conformance.LiteDb", new List<string> { providerTag, "conformance", "litedb" }))
                        suites.Add(conformance);
                    // The LiteGraph backend (graph store over a temporary SQLite file), plus its own suite: edges,
                    // traversal with LiteGraph's client, persistence, deterministic GUIDs, push-down, concurrency, settings.
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(new LiteGraphConformanceTarget(), "Conformance.LiteGraph", new List<string> { providerTag, "conformance", "litegraph" }))
                        suites.Add(conformance);
                    suites.Add(TouchstoneBridge.BuildSuite<LiteGraphBackendTestSuite>(
                        "LiteGraph.Backend", "LiteGraph Backend (Edges / Traversal / Persistence / Push-Down) Tests", () => new LiteGraphBackendTestSuite(), new List<string> { providerTag, "litegraph" }));
                }

                // Backend-neutral RepositoryBase over the in-memory backend (no database; runs in every provider configuration).
                List<string> inMemoryTags = new List<string> { providerTag, "inmemory" };
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryBackendTestSuite>("InMemory.Backend", "In-Memory Backend (CRUD / Writes / Converters / Concurrency) Tests", () => new InMemoryBackendTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryQueryTestSuite>("InMemory.Query", "In-Memory Query Semantics Tests", () => new InMemoryQueryTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryIncludeTestSuite>("InMemory.Include", "In-Memory Include Tests", () => new InMemoryIncludeTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryTransactionTestSuite>("InMemory.Transaction", "In-Memory Transaction / Isolation Tests", () => new InMemoryTransactionTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryCapabilityTestSuite>("InMemory.Capability", "In-Memory Capability Masking Tests", () => new InMemoryCapabilityTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<LiteDbBackendTestSuite>("LiteDb.Backend", "LiteDB Backend (Persistence / Push-down / Precision / Concurrency / Transactions) Tests", () => new LiteDbBackendTestSuite(), new List<string> { providerTag, "litedb" }));
                if (!documentBackend)
                {
                    suites.Add(SharedSuite<InMemorySqlParityTestSuite>("InMemory.SqlParity", "In-Memory vs SQL Parity Tests", providerTag, BeforeEach));
                }

                // Provider-specific unit suites.
                if (configuration.DatabaseType == TestDatabaseType.Sqlite)
                {
                    List<string> sqliteTags = new List<string> { providerTag, "sqlite" };

                    suites.Add(SharedSuite<SqliteIncludeChunkingTestSuite>("Sqlite.IncludeChunking", "SQLite Include Chunking Tests", providerTag, BeforeEach));

                    suites.Add(TouchstoneBridge.BuildSuite<SqliteIntegrationTests>(
                        "Sqlite.Integration", "SQLite Integration Tests", () => new SqliteIntegrationTests(), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<InitializationTests>(
                        "Sqlite.Initialization", "SQLite Initialization Tests", () => new InitializationTests(new NullTestOutputHelper()), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<BooleanConversionTests>(
                        "Sqlite.BooleanConversion", "SQLite Boolean Conversion Tests", () => new BooleanConversionTests(), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<ConcurrencyIntegrationTest>(
                        "Sqlite.ConcurrencyIntegration", "SQLite Concurrency Integration Tests", () => new ConcurrencyIntegrationTest(), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<SqliteInMemoryConcurrencyTests>(
                        "Sqlite.InMemoryConcurrency", "SQLite In-Memory Concurrency Tests", () => new SqliteInMemoryConcurrencyTests(), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<RepositorySettingsTests>(
                        "Sqlite.RepositorySettings", "SQLite Repository Settings Tests", () => new RepositorySettingsTests(), sqliteTags));
                }

                if (!documentBackend)
                {
                    AddTargetSpecificSuites(suites, configuration.DatabaseType, providerTag, BeforeEach);
                }

                return suites;
            }
        }

        #endregion

        #region Private-Methods

        private static void AddSqlProviderSuites(List<TestSuiteDescriptor> suites, string providerTag, System.Func<CancellationToken, Task> beforeEach)
        {
            // Provider-agnostic behavioral suites (exercise the full repository/query surface via IRepositoryProvider).
            suites.Add(SharedSuite<IntegrationTestSuite>("Integration", "Integration Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<DataTypeTestSuite>("DataType", "Data Type Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<IncludeTestSuite>("Include", "Include / Join Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ConcurrencyTestSuite>("Concurrency", "Optimistic Concurrency Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<BatchInsertTestSuite>("BatchInsert", "Batch Insert Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<SchemaManagementTestSuite>("SchemaManagement", "Schema Management Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<MigrationTestSuite>("Migration", "Migration (Introspection / Diff / Sync / Versioned) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<DatabaseLifecycleTestSuite>("DatabaseLifecycle", "Database Lifecycle (CreateDatabaseIfNotExistsAsync / InitializeTablesAsync / ReadTables / AddMigrations) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<DurableToolTestSuite>("DurableTool", "durable Command-Line Tool (Migrate / Script / Schema / Scaffold) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ConnectionPoolStressTestSuite>("ConnectionPoolStress", "Connection Pool Stress Tests", providerTag, beforeEach));

            // Provider-agnostic exhaustive suites authored for Touchstone (positive and negative coverage).
            suites.Add(SharedSuite<ArgumentValidationTestSuite>("ArgumentValidation", "Argument Validation (Negative) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<QueryBuilderTestSuite>("QueryBuilder", "Query Builder Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<AdvancedQueryTestSuite>("AdvancedQuery", "Advanced Query (Set Ops / CTE / Window) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<GroupByTestSuite>("GroupBy", "Group By Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ProjectionTestSuite>("Projection", "Projection / Select Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ComplexExpressionTestSuite>("ComplexExpression", "Complex Expression Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<RelationshipTestSuite>("Relationship", "Relationship / Async Streaming Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<RepositoryOperationsTestSuite>("RepositoryOperations", "Repository Operations (Raw SQL / Batch / Upsert) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<TransactionTestSuite>("Transaction", "Transaction Tests", providerTag, beforeEach));

            // Transactions and infrastructure: ambient scopes, savepoints, external transactions, connection
            // factories, diagnostics (interceptors / tracing / logging / capture), raw SQL and procedures.
            suites.Add(SharedSuite<AmbientTransactionScopeTestSuite>("AmbientTransactionScope", "Ambient Transaction Scope Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<SavepointAndInteropTestSuite>("SavepointInterop", "Savepoint / External Transaction Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ConnectionFactoryTestSuite>("ConnectionFactory", "Connection Factory Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<DiagnosticsTestSuite>("Diagnostics", "Diagnostics (Interceptor / Tracing / Logging / Capture) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<WithQueryTestSuite>("WithQuery", "WithQuery Extensions (Results + Executed SQL) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<RawSqlAndProcedureTestSuite>("RawSqlProcedure", "Raw SQL / Procedure / Timeout / Cancellation Tests", providerTag, beforeEach));

            // Relationship and write-feature suites (split-query Include, many-to-many, composite keys, value
            // converters, JSON, convention mapping, query filters, soft delete, bulk/batch writes, concurrency).
            suites.Add(SharedSuite<ManyToManyTestSuite>("ManyToMany", "Many-to-Many Relationship Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<IncludeCorrectnessTestSuite>("IncludeCorrectness", "Include Correctness Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<CompositeKeyTestSuite>("CompositeKey", "Composite Primary Key Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ValueConverterTestSuite>("ValueConverter", "Value Converter Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<JsonColumnTestSuite>("JsonColumn", "JSON Column Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ConventionMappingTestSuite>("ConventionMapping", "Convention Mapping Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<QueryFilterTestSuite>("QueryFilter", "Global Query Filter Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<SoftDeleteTestSuite>("SoftDelete", "Soft Delete Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<WriteFeaturesTestSuite>("WriteFeatures", "Write Feature (CreateMany / Bulk / Upsert / Batch) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ConcurrencyResolutionTestSuite>("ConcurrencyResolution", "Concurrency Resolution Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<VersionColumnTestSuite>("VersionColumn", "Version Column (Inferred Type / BinaryCounter / Set-Based Bumps) Tests", providerTag, beforeEach));

            suites.Add(SharedSuite<SetOperationTestSuite>("SetOperations", "Set Operation Tests", providerTag, beforeEach));

            // Query translation correctness (predicates, functions, navigation, subqueries, windows, CTEs, projections, paging).
            suites.Add(SharedSuite<QueryTranslationTestSuite>("QueryTranslation", "Query Translation (Predicates / Functions) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<QueryTranslationAdvancedTestSuite>("QueryTranslationAdvanced", "Query Translation (Navigation / Subqueries / Windows / Projections) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<StringMatchingTestSuite>("StringMatching", "String Matching Mode (Ordinal / IgnoreCase / Database) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<WindowFrameTestSuite>("WindowFrame", "Window Frame (ROWS / RANGE / FIRST_VALUE / LAST_VALUE / NTH_VALUE / DENSE_RANK / AVG) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<ProviderRegressionTestSuite>("ProviderRegression", "Provider Regression (Where/Having Grouping / Text Enums / Binary / Custom Keys / Sync Include / Schema-Qualified Tables) Tests", providerTag, beforeEach));
            suites.Add(SharedSuite<MappingSourceTestSuite>("MappingSource", "Entity Mapping Source (Translated Mapping / Includes / Version / Soft Delete / Schema / Non-SQL Backends / Registration Rules) Tests", providerTag, beforeEach));
        }

        private static void AddDocumentBackendSuites(List<TestSuiteDescriptor> suites, string providerTag)
        {
            Task BeforeEach(CancellationToken token) => DurableTestRuntime.EnsureDocumentBackendInitializedAsync(token);

            // The target is created (without connecting) while the suites are built, because the kit reads its capabilities
            // here; BeforeEach connects it before the first case, and DurableTestRuntime.CleanupAsync disposes it.
            IDocumentBackendTestTarget target = DurableTestRuntime.GetDocumentBackend();

            foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(
                target.ConformanceTarget,
                "Conformance",
                new List<string> { providerTag, "conformance", "document" },
                BeforeEach))
            {
                suites.Add(conformance);
            }

            foreach (TestSuiteDescriptor suite in target.BuildBackendSuites(new List<string> { providerTag, "document" }, BeforeEach))
            {
                suites.Add(suite);
            }
        }

        private static void AddTargetSpecificSuites(List<TestSuiteDescriptor> suites, TestDatabaseType databaseType, string providerTag, System.Func<CancellationToken, Task> beforeEach)
        {
            // Provider-specific suites of the v0.7.0 SQL targets: each target adds its suites to its own case (SQLite's
            // are added in All). Document backends return theirs from IDocumentBackendTestTarget.BuildBackendSuites.
            switch (databaseType)
            {
                case TestDatabaseType.Oracle:
                    suites.Add(SharedSuite<OracleProviderTestSuite>("Oracle.Provider", "Oracle Provider (Type Storage / Empty Strings / IN Lists / Bulk / Identifier Case / Scripts) Tests", providerTag, beforeEach));
                    break;

                case TestDatabaseType.DuckDb:
                    break;

                case TestDatabaseType.MariaDb:
                    break;

                case TestDatabaseType.CockroachDb:
                    break;

                case TestDatabaseType.YugabyteDb:
                    break;
            }
        }

        private static TestSuiteDescriptor SharedSuite<T>(
            string suiteId,
            string displayName,
            string providerTag,
            System.Func<CancellationToken, Task> beforeEach) where T : class
        {
            List<string> tags = new List<string> { providerTag, "shared" };
            return TouchstoneBridge.BuildSuite<T>(
                suiteId,
                displayName,
                () => (T)System.Activator.CreateInstance(typeof(T), DurableTestRuntime.RequireProvider())!,
                tags,
                beforeEach);
        }

        #endregion
    }
}
