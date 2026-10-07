namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using Touchstone.Core;
    using Xunit;
    using Xunit.Sdk;

    /// <summary>
    /// Converts xUnit-style test classes (methods annotated with <see cref="FactAttribute"/> or
    /// <see cref="TheoryAttribute"/>) into runner-agnostic Touchstone <see cref="TestCaseDescriptor"/> instances.
    /// This is how the existing Durable test bodies are surfaced identically to the CLI, xUnit, and NUnit runners.
    /// </summary>
    internal static class TouchstoneBridge
    {
        #region Internal-Methods

        /// <summary>
        /// Builds a Touchstone suite from a test class, creating one case per Fact and one case per Theory data row.
        /// </summary>
        /// <typeparam name="T">The test class type.</typeparam>
        /// <param name="suiteId">Stable identifier for the suite.</param>
        /// <param name="displayName">Human-readable suite name.</param>
        /// <param name="instanceFactory">Factory that creates a fresh test-class instance for each case.</param>
        /// <param name="tags">Optional tags applied to every case in the suite.</param>
        /// <param name="beforeEachAsync">Optional delegate awaited before every case (e.g., shared schema setup).</param>
        /// <returns>A populated suite descriptor.</returns>
        public static TestSuiteDescriptor BuildSuite<T>(
            string suiteId,
            string displayName,
            Func<T> instanceFactory,
            IReadOnlyList<string>? tags = null,
            Func<CancellationToken, Task>? beforeEachAsync = null) where T : class
        {
            List<TestCaseDescriptor> cases = BuildCases(typeof(T), suiteId, () => instanceFactory(), tags, beforeEachAsync);
            return new TestSuiteDescriptor(suiteId, displayName, cases);
        }

        /// <summary>
        /// Builds the list of Touchstone cases for a test class.
        /// </summary>
        /// <param name="testClass">The test class type.</param>
        /// <param name="suiteId">Stable identifier for the suite.</param>
        /// <param name="instanceFactory">Factory that creates a fresh test-class instance for each case.</param>
        /// <param name="tags">Optional tags applied to every case.</param>
        /// <param name="beforeEachAsync">Optional delegate awaited before every case.</param>
        /// <returns>The list of cases discovered on the test class.</returns>
        public static List<TestCaseDescriptor> BuildCases(
            Type testClass,
            string suiteId,
            Func<object> instanceFactory,
            IReadOnlyList<string>? tags = null,
            Func<CancellationToken, Task>? beforeEachAsync = null)
        {
            if (testClass == null) throw new ArgumentNullException(nameof(testClass));
            if (instanceFactory == null) throw new ArgumentNullException(nameof(instanceFactory));

            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            MethodInfo[] methods = testClass
                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .OrderBy(m => m.Name, StringComparer.Ordinal)
                .ToArray();

            foreach (MethodInfo method in methods)
            {
                FactAttribute? fact = method.GetCustomAttribute<FactAttribute>();
                if (fact == null)
                {
                    continue;
                }

                bool isTheory = method.GetCustomAttribute<TheoryAttribute>() != null;
                string? skipReason = string.IsNullOrWhiteSpace(fact.Skip) ? null : fact.Skip;
                bool skip = skipReason != null;

                if (isTheory)
                {
                    List<object?[]> dataRows = ResolveTheoryData(method);
                    if (dataRows.Count == 0)
                    {
                        cases.Add(CreateCase(suiteId, method.Name, method.Name, instanceFactory, method, null, tags, beforeEachAsync, skip, skipReason));
                        continue;
                    }

                    int index = 0;
                    foreach (object?[] row in dataRows)
                    {
                        string caseId = method.Name + "(" + index + ")";
                        string displayName = method.Name + "(" + FormatArguments(row) + ")";
                        cases.Add(CreateCase(suiteId, caseId, displayName, instanceFactory, method, row, tags, beforeEachAsync, skip, skipReason));
                        index++;
                    }
                }
                else
                {
                    cases.Add(CreateCase(suiteId, method.Name, method.Name, instanceFactory, method, null, tags, beforeEachAsync, skip, skipReason));
                }
            }

            return cases;
        }

        #endregion

        #region Private-Methods

        private static TestCaseDescriptor CreateCase(
            string suiteId,
            string caseId,
            string displayName,
            Func<object> instanceFactory,
            MethodInfo method,
            object?[]? arguments,
            IReadOnlyList<string>? tags,
            Func<CancellationToken, Task>? beforeEachAsync,
            bool skip,
            string? skipReason)
        {
            Func<CancellationToken, Task> executeAsync = async token =>
            {
                if (beforeEachAsync != null)
                {
                    await beforeEachAsync(token).ConfigureAwait(false);
                }

                token.ThrowIfCancellationRequested();

                object instance = instanceFactory();
                try
                {
                    object? result;
                    try
                    {
                        result = method.Invoke(instance, arguments);
                    }
                    catch (TargetInvocationException ex) when (ex.InnerException != null)
                    {
                        ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                        throw; // unreachable
                    }

                    if (result is Task task)
                    {
                        await task.ConfigureAwait(false);
                    }
                    else if (result is ValueTask valueTask)
                    {
                        await valueTask.ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (!(ex is XunitException) && !(ex is OperationCanceledException))
                {
                    // Runners record only the message; write the full exception so intermittent failures (CI) are diagnosable.
                    Console.Error.WriteLine(DescribeUnexpectedException(suiteId + "." + caseId, ex));
                    throw;
                }
                finally
                {
                    switch (instance)
                    {
                        case IAsyncDisposable asyncDisposable:
                            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                            break;
                        case IDisposable disposable:
                            disposable.Dispose();
                            break;
                    }
                }
            };

            return new TestCaseDescriptor(suiteId, caseId, displayName, executeAsync, tags, skip, skipReason);
        }

        private static string DescribeUnexpectedException(string testId, Exception ex)
        {
            System.Text.StringBuilder text = new System.Text.StringBuilder();
            text.Append("UNEXPECTED EXCEPTION in ").Append(testId).AppendLine(":");
            for (Exception? current = ex; current != null; current = current.InnerException)
            {
                if (current is SqliteException sqlite)
                    text.Append("  SqliteErrorCode=").Append(sqlite.SqliteErrorCode).Append(" SqliteExtendedErrorCode=").Append(sqlite.SqliteExtendedErrorCode).AppendLine();
            }

            text.Append(ex.ToString());
            return text.ToString();
        }

        private static List<object?[]> ResolveTheoryData(MethodInfo method)
        {
            List<object?[]> rows = new List<object?[]>();

            IEnumerable<DataAttribute> dataAttributes = method.GetCustomAttributes<DataAttribute>();
            foreach (DataAttribute dataAttribute in dataAttributes)
            {
                IEnumerable<object?[]>? data = dataAttribute.GetData(method);
                if (data == null)
                {
                    continue;
                }

                foreach (object?[] row in data)
                {
                    rows.Add(row);
                }
            }

            return rows;
        }

        private static string FormatArguments(object?[] arguments)
        {
            if (arguments.Length == 0)
            {
                return string.Empty;
            }

            return string.Join(", ", arguments.Select(FormatArgument));
        }

        private static string FormatArgument(object? argument)
        {
            switch (argument)
            {
                case null:
                    return "null";
                case string text:
                    return "\"" + text + "\"";
                default:
                    return argument.ToString() ?? "null";
            }
        }

        #endregion
    }
}
