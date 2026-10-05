namespace Test.Shared
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
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

                // Set operations (UNION / INTERSECT / EXCEPT) are only supported by the SQLite and PostgreSQL
                // providers; the MySQL and SQL Server implementations do not currently generate correct SQL.
                if (configuration.DatabaseType == TestDatabaseType.Sqlite || configuration.DatabaseType == TestDatabaseType.Postgres)
                {
                    suites.Add(SharedSuite<SetOperationTestSuite>("SetOperations", "Set Operation Tests", providerTag, BeforeEach));
                }

                // Provider-specific unit suites.
                if (configuration.DatabaseType == TestDatabaseType.Sqlite)
                {
                    List<string> sqliteTags = new List<string> { providerTag, "sqlite" };

                    suites.Add(SharedSuite<ManyToManyTestSuite>("ManyToMany", "Many-to-Many Relationship Tests", providerTag, BeforeEach));

                    suites.Add(TouchstoneBridge.BuildSuite<SqliteIntegrationTests>(
                        "Sqlite.Integration", "SQLite Integration Tests", () => new SqliteIntegrationTests(), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<InitializationTests>(
                        "Sqlite.Initialization", "SQLite Initialization Tests", () => new InitializationTests(new NullTestOutputHelper()), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<BooleanConversionTests>(
                        "Sqlite.BooleanConversion", "SQLite Boolean Conversion Tests", () => new BooleanConversionTests(), sqliteTags));
                    suites.Add(TouchstoneBridge.BuildSuite<ConcurrencyIntegrationTest>(
                        "Sqlite.ConcurrencyIntegration", "SQLite Concurrency Integration Tests", () => new ConcurrencyIntegrationTest(), sqliteTags));
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
