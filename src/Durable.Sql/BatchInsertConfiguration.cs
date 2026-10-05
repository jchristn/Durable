namespace Durable.Sql
{
    using System;

    /// <summary>
    /// Batch insert settings. The effective parameters per statement never exceed the dialect's
    /// <see cref="ISqlDialect.MaxParameters"/>.
    /// Thread safety: configure before use; do not mutate while repositories are executing.
    /// </summary>
    public class BatchInsertConfiguration : IBatchInsertConfiguration
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the maximum rows per INSERT statement. Default: 500. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set below 1.</exception>
        public int MaxRowsPerBatch
        {
            get => _MaxRowsPerBatch;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "MaxRowsPerBatch must be at least 1.");
                _MaxRowsPerBatch = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum parameters per statement. Default: 2000. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when set below 1.</exception>
        public int MaxParametersPerStatement
        {
            get => _MaxParametersPerStatement;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "MaxParametersPerStatement must be at least 1.");
                _MaxParametersPerStatement = value;
            }
        }

        /// <summary>
        /// Gets or sets whether prepared statements are reused across batches where the provider benefits. Default: true.
        /// </summary>
        public bool EnablePreparedStatementReuse { get; set; } = true;

        /// <summary>
        /// Gets or sets whether multi-row INSERT syntax is used. When false, rows are inserted one statement at a time. Default: true.
        /// </summary>
        public bool EnableMultiRowInsert { get; set; } = true;

        /// <summary>
        /// Gets a new instance with default values.
        /// </summary>
        public static BatchInsertConfiguration Default => new BatchInsertConfiguration();

        /// <summary>
        /// Gets a new instance tuned for small batches (100 rows, 200 parameters).
        /// </summary>
        public static BatchInsertConfiguration SmallBatch => new BatchInsertConfiguration { MaxRowsPerBatch = 100, MaxParametersPerStatement = 200 };

        /// <summary>
        /// Gets a new instance tuned for large batches (1000 rows).
        /// </summary>
        public static BatchInsertConfiguration LargeBatch => new BatchInsertConfiguration { MaxRowsPerBatch = 1000 };

        /// <summary>
        /// Gets a new instance that inserts one row per statement.
        /// </summary>
        public static BatchInsertConfiguration Compatible => new BatchInsertConfiguration { MaxRowsPerBatch = 1, MaxParametersPerStatement = 50, EnablePreparedStatementReuse = false, EnableMultiRowInsert = false };

        #endregion

        #region Private-Members

        private int _MaxRowsPerBatch = 500;
        private int _MaxParametersPerStatement = 2000;

        #endregion
    }
}
