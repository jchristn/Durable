namespace Durable
{
    using System.Threading;

    /// <summary>
    /// Provides global and thread-local configuration settings for Durable ORM behavior.
    /// Supports hierarchical configuration where thread-local settings override global settings.
    /// </summary>
    public static class DurableConfiguration
    {

        #region Private-Members

        private static volatile bool _GlobalIncludeQuery = false;
        private static readonly AsyncLocal<bool?> _ThreadLocalOverride = new AsyncLocal<bool?>();

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets or sets the global default for whether repository operations should include executed SQL in results.
        /// This setting applies to all repository instances unless overridden by thread-local or instance-level configuration.
        /// Default value is false for performance and backward compatibility.
        /// </summary>
        public static bool GlobalIncludeQuery
        {
            get => _GlobalIncludeQuery;
            set => _GlobalIncludeQuery = value;
        }

        /// <summary>
        /// Gets or sets a thread-local override for SQL inclusion in results.
        /// When set to a non-null value, this overrides the GlobalIncludeQuery setting for the current thread.
        /// When null, the GlobalIncludeQuery setting is used.
        /// This allows temporary per-thread behavior changes without affecting other threads.
        /// </summary>
        public static bool? ThreadLocalIncludeQuery
        {
            get => _ThreadLocalOverride.Value;
            set => _ThreadLocalOverride.Value = value;
        }

        /// <summary>
        /// Gets the effective SQL inclusion setting for the current thread.
        /// Returns the thread-local override if set, otherwise returns the global setting.
        /// This is the computed value that should be used by repository implementations.
        /// </summary>
        public static bool ShouldIncludeQuery => ThreadLocalIncludeQuery ?? GlobalIncludeQuery;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Resolves the effective SQL inclusion setting using explicit configuration hierarchy.
        /// Precedence order (highest to lowest):
        /// 1. Instance-level configuration (repository.IncludeQueryInResults)
        /// 2. Thread-local configuration (ThreadLocalIncludeQuery)
        /// 3. Global configuration (GlobalIncludeQuery)
        /// </summary>
        /// <param name="instanceLevelSetting">The instance-level setting from the repository, or null if not configured</param>
        /// <returns>A tuple containing the effective setting and the source of that setting for debugging</returns>
        /// <exception cref="ArgumentException">This method cannot throw exceptions as all inputs are valid</exception>
        public static (bool EffectiveSetting, string Source) ResolveIncludeQuerySetting(bool? instanceLevelSetting)
        {
            if (instanceLevelSetting.HasValue)
            {
                return (instanceLevelSetting.Value, "Instance");
            }

            if (ThreadLocalIncludeQuery.HasValue)
            {
                return (ThreadLocalIncludeQuery.Value, "ThreadLocal");
            }

            return (GlobalIncludeQuery, "Global");
        }

        #endregion

    }
}