namespace Test.Postgres
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Manual test runner for PostgreSQL integration tests.
    /// This can be used to run tests programmatically outside of a test framework.
    /// </summary>
    public static class PostgresTestRunner
    {
        /// <summary>
        /// Runs all PostgreSQL integration tests and reports results.
        /// </summary>
        /// <returns>A task representing the asynchronous test execution</returns>
        public static async Task RunAllTests()
        {
            Console.WriteLine("====================================================");
            Console.WriteLine("       POSTGRESQL INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();

            int totalTests = 0;
            int passedTests = 0;
            int failedTests = 0;
            int skippedTests = 0;

            // Run integration tests
            TestResults integrationResults = await RunTestClass<PostgresIntegrationTests>("PostgreSQL Integration Tests");
            totalTests += integrationResults.TotalTests;
            passedTests += integrationResults.PassedTests;
            failedTests += integrationResults.FailedTests;
            skippedTests += integrationResults.SkippedTests;

            // Run advanced query builder tests
            TestResults advancedResults = await RunTestClass<PostgresAdvancedQueryBuilderTests>("PostgreSQL Advanced Query Builder Tests");
            totalTests += advancedResults.TotalTests;
            passedTests += advancedResults.PassedTests;
            failedTests += advancedResults.FailedTests;
            skippedTests += advancedResults.SkippedTests;

            // Run include/join functionality tests
            TestResults includeResults = await RunTestClass<PostgresIncludeTests>("PostgreSQL Include/Join Tests");
            totalTests += includeResults.TotalTests;
            passedTests += includeResults.PassedTests;
            failedTests += includeResults.FailedTests;
            skippedTests += includeResults.SkippedTests;

            // Run GROUP BY functionality tests
            TestResults groupByResults = await RunTestClass<PostgresGroupByTests>("PostgreSQL GROUP BY Tests");
            totalTests += groupByResults.TotalTests;
            passedTests += groupByResults.PassedTests;
            failedTests += groupByResults.FailedTests;
            skippedTests += groupByResults.SkippedTests;

            // Run projection functionality tests
            TestResults projectionResults = await RunTestClass<PostgresProjectionTests>("PostgreSQL Projection Tests");
            totalTests += projectionResults.TotalTests;
            passedTests += projectionResults.PassedTests;
            failedTests += projectionResults.FailedTests;
            skippedTests += projectionResults.SkippedTests;

            // Run complex expression functionality tests
            TestResults expressionResults = await RunTestClass<PostgresComplexExpressionTests>("PostgreSQL Complex Expression Tests");
            totalTests += expressionResults.TotalTests;
            passedTests += expressionResults.PassedTests;
            failedTests += expressionResults.FailedTests;
            skippedTests += expressionResults.SkippedTests;

            TestResults transactionResults = await RunTestClass<PostgresTransactionScopeTests>("PostgreSQL Transaction Scope Tests");
            totalTests += transactionResults.TotalTests;
            passedTests += transactionResults.PassedTests;
            failedTests += transactionResults.FailedTests;
            skippedTests += transactionResults.SkippedTests;

            TestResults concurrencyResults = await RunTestClass<PostgresConcurrencyIntegrationTests>("PostgreSQL Concurrency Control Tests");
            totalTests += concurrencyResults.TotalTests;
            passedTests += concurrencyResults.PassedTests;
            failedTests += concurrencyResults.FailedTests;
            skippedTests += concurrencyResults.SkippedTests;

            TestResults dataTypeResults = await RunTestClass<PostgresDataTypeConverterTests>("PostgreSQL Data Type Converter Tests");
            totalTests += dataTypeResults.TotalTests;
            passedTests += dataTypeResults.PassedTests;
            failedTests += dataTypeResults.FailedTests;
            skippedTests += dataTypeResults.SkippedTests;

            TestResults batchInsertResults = await RunTestClass<PostgresBatchInsertTests>("PostgreSQL Performance & Configuration Tests");
            totalTests += batchInsertResults.TotalTests;
            passedTests += batchInsertResults.PassedTests;
            failedTests += batchInsertResults.FailedTests;
            skippedTests += batchInsertResults.SkippedTests;

            TestResults entityRelationshipResults = await RunTestClass<PostgresEntityRelationshipTests>("PostgreSQL Entity Relationships & Complex Models Tests");
            totalTests += entityRelationshipResults.TotalTests;
            passedTests += entityRelationshipResults.PassedTests;
            failedTests += entityRelationshipResults.FailedTests;
            skippedTests += entityRelationshipResults.SkippedTests;

            TestResults repositorySettingsResults = await RunTestClass<PostgresRepositorySettingsTests>("PostgreSQL Repository Settings Tests");
            totalTests += repositorySettingsResults.TotalTests;
            passedTests += repositorySettingsResults.PassedTests;
            failedTests += repositorySettingsResults.FailedTests;
            skippedTests += repositorySettingsResults.SkippedTests;

            TestResults asyncMethodResults = await RunTestClass<PostgresAsyncMethodTests>("PostgreSQL Async Method Tests");
            totalTests += asyncMethodResults.TotalTests;
            passedTests += asyncMethodResults.PassedTests;
            failedTests += asyncMethodResults.FailedTests;
            skippedTests += asyncMethodResults.SkippedTests;

            // Summary
            Console.WriteLine();
            Console.WriteLine("====================================================");
            Console.WriteLine("                 TEST SUMMARY");
            Console.WriteLine("====================================================");
            Console.WriteLine($"Total Tests:   {totalTests}");
            Console.WriteLine($"Passed:        {passedTests} ✅");
            Console.WriteLine($"Failed:        {failedTests} ❌");
            Console.WriteLine($"Skipped:       {skippedTests} ⚠️");
            Console.WriteLine();

            if (failedTests > 0)
            {
                Console.WriteLine("Some tests failed. Check the output above for details.");
                Environment.ExitCode = 1;
            }
            else if (skippedTests > 0)
            {
                Console.WriteLine("All tests passed, but some were skipped (likely due to missing PostgreSQL server).");
                Environment.ExitCode = 0;
            }
            else
            {
                Console.WriteLine("All tests passed! ✅");
                Environment.ExitCode = 0;
            }
        }

        /// <summary>
        /// Runs all test methods in a specific test class.
        /// </summary>
        /// <typeparam name="T">The test class type</typeparam>
        /// <param name="testClassName">Name of the test class for reporting</param>
        /// <returns>Test results containing counts of passed, failed, and skipped tests</returns>
        private static async Task<TestResults> RunTestClass<T>(string testClassName)
        {
            Console.WriteLine($"Running {testClassName}...");
            Console.WriteLine(new string('-', testClassName.Length + 11));

            TestResults results = new TestResults();
            Type testType = typeof(T);

            // Try to create test instance with ITestOutputHelper constructor first, then parameterless
            T testInstance;
            try
            {
                ConstructorInfo? outputHelperConstructor = testType.GetConstructor(new[] { typeof(ITestOutputHelper) });
                if (outputHelperConstructor != null)
                {
                    testInstance = (T)outputHelperConstructor.Invoke(new object[] { new TestOutputHelper() });
                }
                else
                {
                    testInstance = (T)Activator.CreateInstance(testType)!;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create test instance: {ex.Message}");
                return results;
            }

            MethodInfo[] methods = testType.GetMethods(BindingFlags.Public | BindingFlags.Instance);

            foreach (MethodInfo method in methods)
            {
                // Check if method has [Fact] attribute
                FactAttribute? factAttribute = method.GetCustomAttribute<FactAttribute>();
                if (factAttribute == null) continue;

                results.TotalTests++;
                string testName = method.Name;

                try
                {
                    Console.Write($"  {testName}... ");

                    // Execute the test method
                    object? result = method.Invoke(testInstance, null);

                    // Handle async methods
                    if (result is Task task)
                    {
                        await task;
                    }

                    Console.WriteLine("✅ PASSED");
                    results.PassedTests++;
                }
                catch (TargetInvocationException ex) when (ex.InnerException is SkipException)
                {
                    Console.WriteLine("⚠️  SKIPPED");
                    results.SkippedTests++;
                }
                catch (Exception ex)
                {
                    // Get the actual exception (unwrap TargetInvocationException)
                    Exception actualException = ex.InnerException ?? ex;

                    Console.WriteLine($"❌ FAILED: {actualException.Message}");
                    results.FailedTests++;

                    // Print stack trace for debugging
                    Console.WriteLine($"     {actualException.GetType().Name}: {actualException.Message}");
                    if (actualException.StackTrace != null)
                    {
                        string[] stackLines = actualException.StackTrace.Split('\n');
                        foreach (string line in stackLines.Take(3)) // Show first 3 stack trace lines
                        {
                            if (!string.IsNullOrWhiteSpace(line))
                                Console.WriteLine($"     {line.Trim()}");
                        }
                    }
                }
            }

            // Dispose test instance if it implements IDisposable
            if (testInstance is IDisposable disposable)
            {
                disposable.Dispose();
            }

            Console.WriteLine();
            return results;
        }

        /// <summary>
        /// Provides a simple skip exception for test cases that should be skipped.
        /// </summary>
        public class SkipException : Exception
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="SkipException"/> class.
            /// </summary>
            /// <param name="reason">The reason for skipping the test.</param>
            public SkipException(string reason) : base(reason) { }
        }

        /// <summary>
        /// Simple implementation of ITestOutputHelper for the test runner.
        /// </summary>
        private class TestOutputHelper : ITestOutputHelper
        {
            public void WriteLine(string message)
            {
                Console.WriteLine($"     {message}");
            }

            public void WriteLine(string format, params object[] args)
            {
                Console.WriteLine($"     {string.Format(format, args)}");
            }
        }

        /// <summary>
        /// Holds test execution results.
        /// </summary>
        private class TestResults
        {
            public int TotalTests { get; set; }
            public int PassedTests { get; set; }
            public int FailedTests { get; set; }
            public int SkippedTests { get; set; }
        }
    }
}
