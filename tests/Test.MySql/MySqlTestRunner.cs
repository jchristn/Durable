using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Test.MySql
{
    /// <summary>
    /// Manual test runner for MySQL integration tests.
    /// This can be used to run tests programmatically outside of a test framework.
    /// </summary>
    public static class MySqlTestRunner
    {
        /// <summary>
        /// Runs all MySQL integration tests and reports results.
        /// </summary>
        /// <returns>A task representing the asynchronous test execution</returns>
        public static async Task RunAllTests()
        {
            Console.WriteLine("====================================================");
            Console.WriteLine("       MYSQL INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();

            int totalTests = 0;
            int passedTests = 0;
            int failedTests = 0;
            int skippedTests = 0;

            // Run integration tests
            var integrationResults = await RunTestClass<MySqlIntegrationTests>("MySQL Integration Tests");
            totalTests += integrationResults.TotalTests;
            passedTests += integrationResults.PassedTests;
            failedTests += integrationResults.FailedTests;
            skippedTests += integrationResults.SkippedTests;

            // Run advanced query builder tests
            var advancedResults = await RunTestClass<MySqlAdvancedQueryBuilderTests>("MySQL Advanced Query Builder Tests");
            totalTests += advancedResults.TotalTests;
            passedTests += advancedResults.PassedTests;
            failedTests += advancedResults.FailedTests;
            skippedTests += advancedResults.SkippedTests;

            // Run include/join functionality tests
            var includeResults = await RunTestClass<MySqlIncludeTests>("MySQL Include/Join Tests");
            totalTests += includeResults.TotalTests;
            passedTests += includeResults.PassedTests;
            failedTests += includeResults.FailedTests;
            skippedTests += includeResults.SkippedTests;

            // Run GROUP BY functionality tests
            var groupByResults = await RunTestClass<MySqlGroupByTests>("MySQL GROUP BY Tests");
            totalTests += groupByResults.TotalTests;
            passedTests += groupByResults.PassedTests;
            failedTests += groupByResults.FailedTests;
            skippedTests += groupByResults.SkippedTests;

            // Run projection functionality tests
            var projectionResults = await RunTestClass<MySqlProjectionTests>("MySQL Projection Tests");
            totalTests += projectionResults.TotalTests;
            passedTests += projectionResults.PassedTests;
            failedTests += projectionResults.FailedTests;
            skippedTests += projectionResults.SkippedTests;

            // Run complex expression functionality tests
            var expressionResults = await RunTestClass<MySqlComplexExpressionTests>("MySQL Complex Expression Tests");
            totalTests += expressionResults.TotalTests;
            passedTests += expressionResults.PassedTests;
            failedTests += expressionResults.FailedTests;
            skippedTests += expressionResults.SkippedTests;

            var transactionResults = await RunTestClass<MySqlTransactionScopeTests>("MySQL Transaction Scope Tests");
            totalTests += transactionResults.TotalTests;
            passedTests += transactionResults.PassedTests;
            failedTests += transactionResults.FailedTests;
            skippedTests += transactionResults.SkippedTests;

            var concurrencyResults = await RunTestClass<MySqlConcurrencyIntegrationTests>("MySQL Concurrency Control Tests");
            totalTests += concurrencyResults.TotalTests;
            passedTests += concurrencyResults.PassedTests;
            failedTests += concurrencyResults.FailedTests;
            skippedTests += concurrencyResults.SkippedTests;

            var dataTypeResults = await RunTestClass<MySqlDataTypeConverterTests>("MySQL Data Type Converter Tests");
            totalTests += dataTypeResults.TotalTests;
            passedTests += dataTypeResults.PassedTests;
            failedTests += dataTypeResults.FailedTests;
            skippedTests += dataTypeResults.SkippedTests;

            var batchInsertResults = await RunTestClass<MySqlBatchInsertTests>("MySQL Performance & Configuration Tests");
            totalTests += batchInsertResults.TotalTests;
            passedTests += batchInsertResults.PassedTests;
            failedTests += batchInsertResults.FailedTests;
            skippedTests += batchInsertResults.SkippedTests;

            var entityRelationshipResults = await RunTestClass<MySqlEntityRelationshipTests>("MySQL Entity Relationships & Complex Models Tests");
            totalTests += entityRelationshipResults.TotalTests;
            passedTests += entityRelationshipResults.PassedTests;
            failedTests += entityRelationshipResults.FailedTests;
            skippedTests += entityRelationshipResults.SkippedTests;

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
                Console.WriteLine("All tests passed, but some were skipped (likely due to missing MySQL server).");
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

            var results = new TestResults();
            var testType = typeof(T);

            // Try to create test instance with ITestOutputHelper constructor first, then parameterless
            T testInstance;
            try
            {
                var outputHelperConstructor = testType.GetConstructor(new[] { typeof(ITestOutputHelper) });
                if (outputHelperConstructor != null)
                {
                    testInstance = (T)outputHelperConstructor.Invoke(new object[] { new TestOutputHelper() });
                }
                else
                {
                    testInstance = (T)Activator.CreateInstance(testType);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create test instance: {ex.Message}");
                return results;
            }

            var methods = testType.GetMethods(BindingFlags.Public | BindingFlags.Instance);

            foreach (var method in methods)
            {
                // Check if method has [Fact] attribute
                var factAttribute = method.GetCustomAttribute<FactAttribute>();
                if (factAttribute == null) continue;

                results.TotalTests++;
                string testName = method.Name;

                try
                {
                    Console.Write($"  {testName}... ");

                    // Execute the test method
                    var result = method.Invoke(testInstance, null);

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
                    var actualException = ex.InnerException ?? ex;

                    Console.WriteLine($"❌ FAILED: {actualException.Message}");
                    results.FailedTests++;

                    // Print stack trace for debugging
                    Console.WriteLine($"     {actualException.GetType().Name}: {actualException.Message}");
                    if (actualException.StackTrace != null)
                    {
                        var stackLines = actualException.StackTrace.Split('\n');
                        foreach (var line in stackLines.Take(3)) // Show first 3 stack trace lines
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