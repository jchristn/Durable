namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Unified test runner that executes all test suites for a given database provider.
    /// Ensures consistent test execution and reporting across all database implementations.
    /// </summary>
    public static class SharedTestRunner
    {
        #region Public-Methods

        /// <summary>
        /// Runs all test suites for the specified database provider.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database.</param>
        /// <returns>A task representing the asynchronous test execution with exit code (0 = success, 1 = failure).</returns>
        public static async Task<int> RunAllTestsAsync(IRepositoryProvider provider)
        {
            Console.WriteLine("============================================================");
            Console.WriteLine($"          {provider.ProviderName.ToUpper()} INTEGRATION TEST SUITE");
            Console.WriteLine("============================================================");
            Console.WriteLine();

            int totalTests = 0;
            int passedTests = 0;
            int failedTests = 0;
            int skippedTests = 0;
            List<FailureInfo> allFailures = new List<FailureInfo>();

            bool isDatabaseAvailable = await provider.IsDatabaseAvailableAsync();

            if (!isDatabaseAvailable)
            {
                Console.WriteLine($"⚠️  {provider.ProviderName} database is not available. All tests will be skipped.");
                Console.WriteLine();
                Console.WriteLine("====================================================");
                Console.WriteLine("                 TEST SUMMARY");
                Console.WriteLine("====================================================");
                Console.WriteLine($"Total Tests:   0");
                Console.WriteLine($"Passed:        0");
                Console.WriteLine($"Failed:        0");
                Console.WriteLine($"Skipped:       0 (Database not available)");
                Console.WriteLine();
                return 0;
            }

            try
            {
                await provider.SetupDatabaseAsync();

                TestResults integrationResults = await RunTestClass<IntegrationTestSuite>(
                    $"{provider.ProviderName} Integration Tests",
                    provider
                );
                totalTests += integrationResults.TotalTests;
                passedTests += integrationResults.PassedTests;
                failedTests += integrationResults.FailedTests;
                skippedTests += integrationResults.SkippedTests;
                allFailures.AddRange(integrationResults.Failures);

                TestResults dataTypeResults = await RunTestClass<DataTypeTestSuite>(
                    $"{provider.ProviderName} Data Type Tests",
                    provider
                );
                totalTests += dataTypeResults.TotalTests;
                passedTests += dataTypeResults.PassedTests;
                failedTests += dataTypeResults.FailedTests;
                skippedTests += dataTypeResults.SkippedTests;
                allFailures.AddRange(dataTypeResults.Failures);

                TestResults includeResults = await RunTestClass<IncludeTestSuite>(
                    $"{provider.ProviderName} Include/Join Tests",
                    provider
                );
                totalTests += includeResults.TotalTests;
                passedTests += includeResults.PassedTests;
                failedTests += includeResults.FailedTests;
                skippedTests += includeResults.SkippedTests;
                allFailures.AddRange(includeResults.Failures);

                TestResults concurrencyResults = await RunTestClass<ConcurrencyTestSuite>(
                    $"{provider.ProviderName} Concurrency Tests",
                    provider
                );
                totalTests += concurrencyResults.TotalTests;
                passedTests += concurrencyResults.PassedTests;
                failedTests += concurrencyResults.FailedTests;
                skippedTests += concurrencyResults.SkippedTests;
                allFailures.AddRange(concurrencyResults.Failures);

                TestResults batchResults = await RunTestClass<BatchInsertTestSuite>(
                    $"{provider.ProviderName} Batch Insert Tests",
                    provider
                );
                totalTests += batchResults.TotalTests;
                passedTests += batchResults.PassedTests;
                failedTests += batchResults.FailedTests;
                skippedTests += batchResults.SkippedTests;
                allFailures.AddRange(batchResults.Failures);

                TestResults schemaResults = await RunTestClass<SchemaManagementTestSuite>(
                    $"{provider.ProviderName} Schema Management Tests",
                    provider
                );
                totalTests += schemaResults.TotalTests;
                passedTests += schemaResults.PassedTests;
                failedTests += schemaResults.FailedTests;
                skippedTests += schemaResults.SkippedTests;
                allFailures.AddRange(schemaResults.Failures);

                TestResults poolResults = await RunTestClass<ConnectionPoolStressTestSuite>(
                    $"{provider.ProviderName} Connection Pool Stress Tests",
                    provider
                );
                totalTests += poolResults.TotalTests;
                passedTests += poolResults.PassedTests;
                failedTests += poolResults.FailedTests;
                skippedTests += poolResults.SkippedTests;
                allFailures.AddRange(poolResults.Failures);
            }
            finally
            {
                await provider.CleanupDatabaseAsync();
            }

            Console.WriteLine();
            Console.WriteLine("============================================================");
            Console.WriteLine("                       TEST SUMMARY");
            Console.WriteLine("============================================================");
            Console.WriteLine($"  Total:    {totalTests,4}");
            Console.WriteLine($"  Passed:   {passedTests,4}");
            Console.WriteLine($"  Failed:   {failedTests,4}");
            Console.WriteLine($"  Skipped:  {skippedTests,4}");
            Console.WriteLine("------------------------------------------------------------");

            if (failedTests > 0)
            {
                Console.WriteLine($"  Result:   FAILED - {failedTests} test(s) failed");
                Console.WriteLine("============================================================");
                Console.WriteLine();
                Console.WriteLine("============================================================");
                Console.WriteLine("                    FAILED TESTS DETAILS");
                Console.WriteLine("============================================================");

                for (int i = 0; i < allFailures.Count; i++)
                {
                    FailureInfo failure = allFailures[i];
                    Console.WriteLine();
                    Console.WriteLine($"  [{i + 1}] {failure.TestName}");
                    Console.WriteLine($"      Suite: {failure.TestSuite}");
                    Console.WriteLine($"      Error: {failure.ExceptionType}");
                    Console.WriteLine($"      Message: {failure.Message}");
                    if (!string.IsNullOrEmpty(failure.StackTrace))
                    {
                        Console.WriteLine("      Stack Trace:");
                        string[] stackLines = failure.StackTrace.Split('\n');
                        foreach (string line in stackLines.Take(5))
                        {
                            if (!string.IsNullOrWhiteSpace(line))
                                Console.WriteLine($"        {line.Trim()}");
                        }
                    }
                }

                Console.WriteLine();
                Console.WriteLine("============================================================");
                return 1;
            }
            else if (skippedTests > 0)
            {
                Console.WriteLine($"  Result:   PASSED with warnings - {skippedTests} test(s) skipped");
                Console.WriteLine("============================================================");
                return 0;
            }
            else
            {
                Console.WriteLine("  Result:   ALL TESTS PASSED");
                Console.WriteLine("============================================================");
                return 0;
            }
        }

        #endregion

        #region Private-Methods

        private static async Task<TestResults> RunTestClass<T>(string testClassName, IRepositoryProvider provider) where T : class
        {
            Console.WriteLine($"Running {testClassName}...");
            Console.WriteLine(new string('-', 60));

            TestResults results = new TestResults();
            results.TestSuiteName = testClassName;
            Type testType = typeof(T);

            T? testInstance;
            try
            {
                ConstructorInfo? providerConstructor = testType.GetConstructor(new[] { typeof(IRepositoryProvider) });
                if (providerConstructor != null)
                {
                    testInstance = (T)providerConstructor.Invoke(new object[] { provider });
                }
                else
                {
                    ConstructorInfo? outputHelperConstructor = testType.GetConstructor(new[] { typeof(ITestOutputHelper) });
                    if (outputHelperConstructor != null)
                    {
                        testInstance = (T)outputHelperConstructor.Invoke(new object[] { new TestOutputHelper() });
                    }
                    else
                    {
                        testInstance = (T?)Activator.CreateInstance(testType);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  SETUP FAILED: {ex.Message}");
                return results;
            }

            if (testInstance == null)
            {
                Console.WriteLine($"  SETUP FAILED: instance is null");
                return results;
            }

            MethodInfo[] methods = testType.GetMethods(BindingFlags.Public | BindingFlags.Instance);

            // Calculate max test name length for alignment (cap at 45 chars)
            int maxNameLength = 45;

            foreach (MethodInfo method in methods)
            {
                FactAttribute? factAttribute = method.GetCustomAttribute<FactAttribute>();
                if (factAttribute == null) continue;

                results.TotalTests++;
                string testName = method.Name;

                // Truncate long test names for display
                string displayName = testName.Length > maxNameLength
                    ? testName.Substring(0, maxNameLength - 3) + "..."
                    : testName;

                try
                {
                    // Capture test output
                    StringWriter testOutput = new StringWriter();
                    TextWriter originalOut = Console.Out;
                    Console.SetOut(testOutput);

                    try
                    {
                        object? result = method.Invoke(testInstance, null);

                        if (result is Task task)
                        {
                            await task;
                        }
                    }
                    finally
                    {
                        Console.SetOut(originalOut);
                    }

                    // Write result line first
                    Console.WriteLine($"  {displayName.PadRight(maxNameLength)} PASSED");
                    results.PassedTests++;

                    // Then write captured output if any (indented)
                    string captured = testOutput.ToString();
                    if (!string.IsNullOrWhiteSpace(captured))
                    {
                        string[] lines = captured.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (string line in lines)
                        {
                            Console.WriteLine($"       {line}");
                        }
                    }
                }
                catch (TargetInvocationException ex) when (ex.InnerException is SkipException)
                {
                    Console.WriteLine($"  {displayName.PadRight(maxNameLength)} SKIPPED");
                    results.SkippedTests++;
                }
                catch (Exception ex)
                {
                    Exception actualException = ex.InnerException ?? ex;

                    Console.WriteLine($"  {displayName.PadRight(maxNameLength)} FAILED");
                    results.FailedTests++;

                    Console.WriteLine($"       Error: {actualException.GetType().Name}");
                    Console.WriteLine($"       {actualException.Message}");
                    if (actualException.StackTrace != null)
                    {
                        string[] stackLines = actualException.StackTrace.Split('\n');
                        foreach (string line in stackLines.Take(3))
                        {
                            if (!string.IsNullOrWhiteSpace(line))
                                Console.WriteLine($"       {line.Trim()}");
                        }
                    }

                    results.Failures.Add(new FailureInfo
                    {
                        TestName = testName,
                        TestSuite = results.TestSuiteName,
                        ExceptionType = actualException.GetType().Name,
                        Message = actualException.Message,
                        StackTrace = actualException.StackTrace ?? string.Empty
                    });
                }
            }

            if (testInstance is IDisposable disposable)
            {
                disposable.Dispose();
            }

            Console.WriteLine();
            return results;
        }

        #endregion

        #region Private-Classes

        /// <summary>
        /// Exception thrown when a test should be skipped.
        /// </summary>
        public class SkipException : Exception
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="SkipException"/> class.
            /// </summary>
            /// <param name="reason">The reason for skipping the test.</param>
            public SkipException(string reason) : base(reason) { }
        }

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

        private class TestResults
        {
            public string TestSuiteName { get; set; } = string.Empty;
            public int TotalTests { get; set; }
            public int PassedTests { get; set; }
            public int FailedTests { get; set; }
            public int SkippedTests { get; set; }
            public List<FailureInfo> Failures { get; } = new List<FailureInfo>();
        }

        private class FailureInfo
        {
            public string TestName { get; set; } = string.Empty;
            public string TestSuite { get; set; } = string.Empty;
            public string ExceptionType { get; set; } = string.Empty;
            public string Message { get; set; } = string.Empty;
            public string StackTrace { get; set; } = string.Empty;
        }

        #endregion
    }
}
