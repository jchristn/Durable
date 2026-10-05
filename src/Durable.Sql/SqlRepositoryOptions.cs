namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Options for SQL repositories.
    /// Thread safety: configure before constructing repositories; do not mutate while repositories are in use.
    /// </summary>
    public class SqlRepositoryOptions : RepositoryOptions
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets batch insert settings. Default: <see cref="BatchInsertConfiguration.Default"/>. Never null.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when set to null.</exception>
        public IBatchInsertConfiguration BatchConfiguration
        {
            get => _BatchConfiguration;
            set => _BatchConfiguration = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Gets or sets a data type converter replacing the provider's default. Default: null (provider default).
        /// </summary>
        public IDataTypeConverter? DataTypeConverter { get; set; } = null;

        /// <summary>
        /// Gets or sets the command timeout in seconds, or null for the driver default. Minimum: 0 (no timeout).
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set to a negative value.</exception>
        public int? CommandTimeoutSeconds
        {
            get => _CommandTimeoutSeconds;
            set
            {
                if (value.HasValue && value.Value < 0) throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeoutSeconds cannot be negative.");
                _CommandTimeoutSeconds = value;
            }
        }

        /// <summary>
        /// Gets the command interceptors, invoked in order. Never null.
        /// </summary>
        public IList<ISqlCommandInterceptor> Interceptors { get; } = new List<ISqlCommandInterceptor>();

        /// <summary>
        /// Gets or sets the initial value of <see cref="ISqlCapture.CaptureSql"/>. Default: false.
        /// </summary>
        public bool CaptureSql { get; set; } = false;

        /// <summary>
        /// Gets or sets the initial value of <see cref="ISqlTrackingConfiguration.IncludeQueryInResults"/>. Default: false.
        /// </summary>
        public bool IncludeQueryInResults { get; set; } = false;

        /// <summary>
        /// Gets or sets how many root entities are buffered before loading includes while streaming with
        /// <c>ExecuteAsyncEnumerable</c>. Default: 256. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set below 1.</exception>
        public int IncludeStreamingBatchSize
        {
            get => _IncludeStreamingBatchSize;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "IncludeStreamingBatchSize must be at least 1.");
                _IncludeStreamingBatchSize = value;
            }
        }

        #endregion

        #region Private-Members

        private IBatchInsertConfiguration _BatchConfiguration = BatchInsertConfiguration.Default;
        private int? _CommandTimeoutSeconds = null;
        private int _IncludeStreamingBatchSize = 256;

        #endregion
    }
}
