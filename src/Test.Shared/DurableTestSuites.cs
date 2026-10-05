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
    /// their owning provider.
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
                string providerTag = ProviderTag(configuration.DatabaseType);

                Task BeforeEach(CancellationToken token) => DurableTestRuntime.EnsureInitializedAsync(token);

                List<TestSuiteDescriptor> suites = new List<TestSuiteDescriptor>();

                // Provider-agnostic behavioral suites (exercise the full repository/query surface via IRepositoryProvider).
                suites.Add(SharedSuite<IntegrationTestSuite>("Integration", "Integration Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<DataTypeTestSuite>("DataType", "Data Type Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<IncludeTestSuite>("Include", "Include / Join Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ConcurrencyTestSuite>("Concurrency", "Optimistic Concurrency Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<BatchInsertTestSuite>("BatchInsert", "Batch Insert Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<SchemaManagementTestSuite>("SchemaManagement", "Schema Management Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<MigrationTestSuite>("Migration", "Migration (Introspection / Diff / Sync / Versioned) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ConnectionPoolStressTestSuite>("ConnectionPoolStress", "Connection Pool Stress Tests", providerTag, BeforeEach));

                // Provider-agnostic exhaustive suites authored for Touchstone (positive and negative coverage).
                suites.Add(SharedSuite<ArgumentValidationTestSuite>("ArgumentValidation", "Argument Validation (Negative) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<QueryBuilderTestSuite>("QueryBuilder", "Query Builder Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<AdvancedQueryTestSuite>("AdvancedQuery", "Advanced Query (Set Ops / CTE / Window) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<GroupByTestSuite>("GroupBy", "Group By Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ProjectionTestSuite>("Projection", "Projection / Select Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ComplexExpressionTestSuite>("ComplexExpression", "Complex Expression Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<RelationshipTestSuite>("Relationship", "Relationship / Async Streaming Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<RepositoryOperationsTestSuite>("RepositoryOperations", "Repository Operations (Raw SQL / Batch / Upsert) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<TransactionTestSuite>("Transaction", "Transaction Tests", providerTag, BeforeEach));

                // Transactions and infrastructure: ambient scopes, savepoints, external transactions, connection
                // factories, diagnostics (interceptors / tracing / logging / capture), raw SQL and procedures.
                suites.Add(SharedSuite<TransactionScopeTestSuite>("TransactionScope", "Transaction Scope Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<SavepointAndInteropTestSuite>("SavepointInterop", "Savepoint / External Transaction Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ConnectionFactoryTestSuite>("ConnectionFactory", "Connection Factory Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<DiagnosticsTestSuite>("Diagnostics", "Diagnostics (Interceptor / Tracing / Logging / Capture) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<RawSqlAndProcedureTestSuite>("RawSqlProcedure", "Raw SQL / Procedure / Timeout / Cancellation Tests", providerTag, BeforeEach));

                // Relationship and write-feature suites (split-query Include, many-to-many, composite keys, value
                // converters, JSON, convention mapping, query filters, soft delete, bulk/batch writes, concurrency).
                suites.Add(SharedSuite<ManyToManyTestSuite>("ManyToMany", "Many-to-Many Relationship Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<IncludeCorrectnessTestSuite>("IncludeCorrectness", "Include Correctness Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<CompositeKeyTestSuite>("CompositeKey", "Composite Primary Key Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ValueConverterTestSuite>("ValueConverter", "Value Converter Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<JsonColumnTestSuite>("JsonColumn", "JSON Column Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ConventionMappingTestSuite>("ConventionMapping", "Convention Mapping Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<QueryFilterTestSuite>("QueryFilter", "Global Query Filter Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<SoftDeleteTestSuite>("SoftDelete", "Soft Delete Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<WriteFeaturesTestSuite>("WriteFeatures", "Write Feature (CreateMany / Bulk / Upsert / Batch) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<ConcurrencyResolutionTestSuite>("ConcurrencyResolution", "Concurrency Resolution Tests", providerTag, BeforeEach));

                suites.Add(SharedSuite<SetOperationTestSuite>("SetOperations", "Set Operation Tests", providerTag, BeforeEach));

                // Query translation correctness (predicates, functions, navigation, subqueries, windows, CTEs, projections, paging).
                suites.Add(SharedSuite<QueryTranslationTestSuite>("QueryTranslation", "Query Translation (Predicates / Functions) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<QueryTranslationAdvancedTestSuite>("QueryTranslationAdvanced", "Query Translation (Navigation / Subqueries / Windows / Projections) Tests", providerTag, BeforeEach));
                suites.Add(SharedSuite<StringMatchingTestSuite>("StringMatching", "String Matching Mode (Ordinal / IgnoreCase / Database) Tests", providerTag, BeforeEach));

                // Backend-neutral unit suites (no database).
                suites.Add(TouchstoneBridge.BuildSuite<QueryNormalizerTestSuite>(
                    "QueryNormalizer", "Query Normalizer (Neutral Query Model) Tests", () => new QueryNormalizerTestSuite(), new List<string> { providerTag, "neutral" }));

                // Durable.Conformance kit: the backend-neutral suites every IRepository<T> backend must pass, run here
                // against the configured SQL provider (SqlConformanceTarget resets storage by dropping and recreating tables).
                foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(
                    new SqlConformanceTarget(ProviderName(configuration.DatabaseType), DurableTestRuntime.RequireProvider),
                    "Conformance",
                    new List<string> { providerTag, "conformance" },
                    BeforeEach))
                {
                    suites.Add(conformance);
                }

                // The same kit against the in-memory backend (no database), once in the default SQLite configuration, and
                // against a deliberately limited in-memory backend to prove unsupported calls fail at the call site.
                if (configuration.DatabaseType == TestDatabaseType.Sqlite)
                {
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(new InMemoryConformanceTarget(), "Conformance.InMemory", new List<string> { providerTag, "conformance", "inmemory" }))
                        suites.Add(conformance);
                    foreach (TestSuiteDescriptor conformance in ConformanceSuites.Build(new InMemoryConformanceTarget(RepositoryCapabilities.None), "Conformance.InMemoryMinimal", new List<string> { providerTag, "conformance", "inmemory" }))
                        suites.Add(conformance);
                }

                // Backend-neutral RepositoryBase over the in-memory backend (no database; runs in every provider configuration).
                List<string> inMemoryTags = new List<string> { providerTag, "inmemory" };
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryBackendTestSuite>("InMemory.Backend", "In-Memory Backend (CRUD / Writes / Converters / Concurrency) Tests", () => new InMemoryBackendTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryQueryTestSuite>("InMemory.Query", "In-Memory Query Semantics Tests", () => new InMemoryQueryTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryIncludeTestSuite>("InMemory.Include", "In-Memory Include Tests", () => new InMemoryIncludeTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryTransactionTestSuite>("InMemory.Transaction", "In-Memory Transaction / Isolation Tests", () => new InMemoryTransactionTestSuite(), inMemoryTags));
                suites.Add(TouchstoneBridge.BuildSuite<InMemoryCapabilityTestSuite>("InMemory.Capability", "In-Memory Capability Masking Tests", () => new InMemoryCapabilityTestSuite(), inMemoryTags));
                suites.Add(SharedSuite<InMemorySqlParityTestSuite>("InMemory.SqlParity", "In-Memory vs SQL Parity Tests", providerTag, BeforeEach));

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

                return suites;
            }
        }

        #endregion

        #region Private-Methods

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

        private static string ProviderName(TestDatabaseType databaseType)
        {
            switch (databaseType)
            {
                case TestDatabaseType.Sqlite: return "SQLite";
                case TestDatabaseType.MySql: return "MySQL";
                case TestDatabaseType.Postgres: return "PostgreSQL";
                case TestDatabaseType.SqlServer: return "SQL Server";
                default: return "Unknown";
            }
        }

        private static string ProviderTag(TestDatabaseType databaseType)
        {
            switch (databaseType)
            {
                case TestDatabaseType.Sqlite: return "sqlite";
                case TestDatabaseType.MySql: return "mysql";
                case TestDatabaseType.Postgres: return "postgres";
                case TestDatabaseType.SqlServer: return "sqlserver";
                default: return "unknown";
            }
        }

        #endregion
    }
}
