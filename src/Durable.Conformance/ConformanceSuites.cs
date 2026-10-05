namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Touchstone.Core;

    /// <summary>
    /// Entry point of the conformance kit: builds runner-agnostic Touchstone suites (one per feature area) for a backend.
    /// Run them with any Touchstone runner (console, xUnit adapter, NUnit adapter), for example
    /// <c>await ConsoleRunner.RunAsync(ConformanceSuites.Build(new MyTarget()))</c>.
    /// Thread safety: all members are stateless and safe for concurrent use; the returned cases must run sequentially.
    /// </summary>
    public static class ConformanceSuites
    {
        #region Public-Members

        /// <summary>
        /// Gets the default suite id prefix ("Conformance"); suite ids are <c>prefix + "." + area</c>.
        /// </summary>
        public static string DefaultSuiteIdPrefix { get; } = "Conformance";

        #endregion

        #region Private-Members

        private static readonly IReadOnlyList<ConformanceSuiteInfo> _Suites = new List<ConformanceSuiteInfo>
        {
            new ConformanceSuiteInfo("Crud", "CRUD and Reads", typeof(CrudSuite)),
            new ConformanceSuiteInfo("DataTypes", "Data Type Round-Trips", typeof(DataTypeSuite)),
            new ConformanceSuiteInfo("Predicates", "Predicates (Comparisons / Nulls / Enums / IN / Strings / Arithmetic)", typeof(PredicateSuite)),
            new ConformanceSuiteInfo("Functions", "Functions (String / Date / Math)", typeof(FunctionSuite)),
            new ConformanceSuiteInfo("OrderingPaging", "Ordering and Paging", typeof(OrderingPagingSuite)),
            new ConformanceSuiteInfo("Aggregates", "Aggregates", typeof(AggregateSuite)),
            new ConformanceSuiteInfo("Include", "Include / ThenInclude / Many-to-Many", typeof(IncludeSuite)),
            new ConformanceSuiteInfo("NavigationPredicates", "Navigation Predicates", typeof(NavigationPredicateSuite)),
            new ConformanceSuiteInfo("Grouping", "Grouping", typeof(GroupingSuite)),
            new ConformanceSuiteInfo("Projection", "Projection and Distinct", typeof(ProjectionSuite)),
            new ConformanceSuiteInfo("QueryFilters", "Query Filters", typeof(QueryFilterSuite)),
            new ConformanceSuiteInfo("SoftDelete", "Soft Delete", typeof(SoftDeleteSuite)),
            new ConformanceSuiteInfo("CompositeKeys", "Composite Keys", typeof(CompositeKeySuite)),
            new ConformanceSuiteInfo("ValueConverters", "Value Converters and Convention Mapping", typeof(ValueConverterSuite)),
            new ConformanceSuiteInfo("Concurrency", "Optimistic Concurrency and Conflict Resolvers", typeof(ConcurrencySuite)),
            new ConformanceSuiteInfo("Writes", "Batch Writes (CreateMany / UpdateMany / UpdateField / BatchUpdate / BatchDelete)", typeof(WriteSuite)),
            new ConformanceSuiteInfo("Upsert", "Upsert", typeof(UpsertSuite)),
            new ConformanceSuiteInfo("Transactions", "Transactions", typeof(TransactionSuite)),
            new ConformanceSuiteInfo("StringMatching", "String Matching Modes", typeof(StringMatchingSuite)),
            new ConformanceSuiteInfo("Capabilities", "Capabilities (supported features work, missing ones throw NotSupportedException)", typeof(CapabilitySuite))
        };

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds every conformance suite for a target. Cases whose required capabilities the target lacks are included as
        /// skipped cases with a reason; the "Capabilities" suite always runs one case per capability flag.
        /// </summary>
        /// <param name="target">Backend under test. Must not be null.</param>
        /// <param name="suiteIdPrefix">Suite id prefix; use distinct prefixes when registering several targets in one run. Null or empty uses <see cref="DefaultSuiteIdPrefix"/>.</param>
        /// <param name="tags">Optional tags applied to every case; null for none.</param>
        /// <param name="beforeEachAsync">Optional delegate awaited before every case (for example to start a server); null for none.</param>
        /// <returns>The suites, in a stable order. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when target is null.</exception>
        public static IReadOnlyList<TestSuiteDescriptor> Build(
            IConformanceTarget target,
            string? suiteIdPrefix = null,
            IReadOnlyList<string>? tags = null,
            Func<CancellationToken, Task>? beforeEachAsync = null)
        {
            ArgumentNullException.ThrowIfNull(target);
            string prefix = string.IsNullOrEmpty(suiteIdPrefix) ? DefaultSuiteIdPrefix : suiteIdPrefix;
            List<TestSuiteDescriptor> suites = new List<TestSuiteDescriptor>();
            foreach (ConformanceSuiteInfo info in _Suites)
            {
                suites.Add(BuildSuite(info.SuiteType, prefix + "." + info.Id, "Conformance (" + target.Name + "): " + info.DisplayName, target, tags, beforeEachAsync));
            }

            return suites;
        }

        /// <summary>
        /// Builds one suite from a <see cref="ConformanceSuite"/> subclass (one case per <see cref="ConformanceTestAttribute"/>
        /// method, ordered by method name). Use it to run additional backend-specific suites the same way as the built-in ones.
        /// </summary>
        /// <param name="suiteType">A non-abstract <see cref="ConformanceSuite"/> subclass with a public constructor taking an <see cref="IConformanceTarget"/>. Must not be null.</param>
        /// <param name="suiteId">Stable suite id. Must not be null or empty.</param>
        /// <param name="displayName">Display name. Must not be null or empty.</param>
        /// <param name="target">Backend under test. Must not be null.</param>
        /// <param name="tags">Optional tags applied to every case; null for none.</param>
        /// <param name="beforeEachAsync">Optional delegate awaited before every case; null for none.</param>
        /// <returns>The suite. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null or empty.</exception>
        /// <exception cref="ArgumentException">Thrown when suiteType is not a usable suite class.</exception>
        public static TestSuiteDescriptor BuildSuite(
            Type suiteType,
            string suiteId,
            string displayName,
            IConformanceTarget target,
            IReadOnlyList<string>? tags = null,
            Func<CancellationToken, Task>? beforeEachAsync = null)
        {
            ArgumentNullException.ThrowIfNull(suiteType);
            ArgumentNullException.ThrowIfNull(target);
            if (string.IsNullOrEmpty(suiteId)) throw new ArgumentNullException(nameof(suiteId));
            if (string.IsNullOrEmpty(displayName)) throw new ArgumentNullException(nameof(displayName));
            if (suiteType.IsAbstract || !typeof(ConformanceSuite).IsAssignableFrom(suiteType))
                throw new ArgumentException("Suite type " + suiteType.FullName + " must be a non-abstract ConformanceSuite subclass.", nameof(suiteType));

            ConstructorInfo? constructor = suiteType.GetConstructor(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { typeof(IConformanceTarget) }, null);
            if (constructor == null)
                throw new ArgumentException("Suite type " + suiteType.FullName + " needs a constructor taking an IConformanceTarget.", nameof(suiteType));

            RepositoryCapabilities available = target.Capabilities;
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();
            MethodInfo[] methods = suiteType
                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .OrderBy(m => m.Name, StringComparer.Ordinal)
                .ToArray();

            foreach (MethodInfo method in methods)
            {
                ConformanceTestAttribute? attribute = method.GetCustomAttribute<ConformanceTestAttribute>();
                if (attribute == null) continue;
                if (method.GetParameters().Length != 0)
                    throw new ArgumentException("Conformance test " + suiteType.Name + "." + method.Name + " must not take parameters.", nameof(suiteType));

                RepositoryCapabilities missing = attribute.Requires & ~available;
                bool skip = missing != RepositoryCapabilities.None;
                string? skipReason = skip
                    ? "Requires " + missing + ", which target '" + target.Name + "' does not support; the Capabilities suite verifies these operations throw NotSupportedException."
                    : null;
                string caseDisplay = string.IsNullOrEmpty(attribute.Description) ? method.Name : method.Name + " - " + attribute.Description;
                cases.Add(new TestCaseDescriptor(suiteId, method.Name, caseDisplay, CreateExecutor(constructor, method, target, beforeEachAsync), tags, skip, skipReason));
            }

            return new TestSuiteDescriptor(suiteId, displayName, cases);
        }

        #endregion

        #region Private-Methods

        private static Func<CancellationToken, Task> CreateExecutor(
            ConstructorInfo constructor,
            MethodInfo method,
            IConformanceTarget target,
            Func<CancellationToken, Task>? beforeEachAsync)
        {
            return async token =>
            {
                if (beforeEachAsync != null) await beforeEachAsync(token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();

                ConformanceSuite suite = (ConformanceSuite)constructor.Invoke(new object[] { target });
                try
                {
                    suite.Token = token;
                    object? result;
                    try
                    {
                        result = method.Invoke(suite, null);
                    }
                    catch (TargetInvocationException ex) when (ex.InnerException != null)
                    {
                        ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                        throw;
                    }

                    if (result is Task task) await task.ConfigureAwait(false);
                }
                finally
                {
                    suite.Dispose();
                }
            };
        }

        #endregion
    }
}
