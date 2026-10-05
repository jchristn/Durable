namespace Durable
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Backend-neutral repository options. Backends extend this type with their own settings.
    /// Thread safety: configure before constructing repositories; do not mutate while repositories are in use.
    /// </summary>
    public class RepositoryOptions
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the logger. When set, executed commands are logged at Debug level, slow commands at Warning,
        /// and failures at Error. Default: null (no logging).
        /// </summary>
        public ILogger? Logger { get; set; } = null;

        /// <summary>
        /// Gets or sets whether parameter values are included in logs and trace tags.
        /// Leave disabled when values may contain sensitive data. Default: false.
        /// </summary>
        public bool LogParameterValues { get; set; } = false;

        /// <summary>
        /// Gets or sets how string comparisons without an explicit <see cref="StringComparison"/> argument are evaluated
        /// (<c>==</c>, <c>!=</c>, ordering, <c>Contains</c>/<c>StartsWith</c>/<c>EndsWith</c>, collection <c>Contains</c>,
        /// <c>Replace</c>, <c>IndexOf</c>). Calls that pass a <see cref="StringComparison"/> always use the mode it requests.
        /// Default: <see cref="StringMatchMode.Database"/> (the backend's collation).
        /// </summary>
        public StringMatchMode StringMatching { get; set; } = StringMatchMode.Database;

        /// <summary>
        /// Gets or sets the duration above which a command is logged as slow at Warning level.
        /// Null disables slow-command logging. Default: 1 second. Minimum: <see cref="TimeSpan.Zero"/>.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set to a negative duration.</exception>
        public TimeSpan? SlowCommandThreshold
        {
            get => _SlowCommandThreshold;
            set
            {
                if (value.HasValue && value.Value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(value), "SlowCommandThreshold cannot be negative.");
                _SlowCommandThreshold = value;
            }
        }

        #endregion

        #region Private-Members

        private TimeSpan? _SlowCommandThreshold = TimeSpan.FromSeconds(1);

        #endregion
    }
}
