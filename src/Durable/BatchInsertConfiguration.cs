namespace Durable
{
    /// <summary>
    /// Default implementation of batch insert configuration with sensible defaults.
    /// </summary>
    public class BatchInsertConfiguration : IBatchInsertConfiguration
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the maximum number of rows to include in a single multi-row INSERT statement.
        /// Default is 500 rows per batch, which provides good performance without hitting parameter limits.
        /// </summary>
        public int MaxRowsPerBatch { get; set; } = 500;

        /// <summary>
        /// Gets or sets the maximum number of parameters per INSERT statement.
        /// Default is 900 to stay well under SQLite's default limit of 999 parameters.
        /// </summary>
        public int MaxParametersPerStatement { get; set; } = 900;

        /// <summary>
        /// Gets or sets whether to use prepared statement reuse for batch operations.
        /// Default is true for better performance.
        /// </summary>
        public bool EnablePreparedStatementReuse { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to use multi-row INSERT syntax when possible.
        /// Default is true for better performance.
        /// </summary>
        public bool EnableMultiRowInsert { get; set; } = true;

        /// <summary>
        /// Creates a new instance with default values optimized for SQLite.
        /// </summary>
        public static BatchInsertConfiguration Default => new BatchInsertConfiguration();

        /// <summary>
        /// Creates a configuration optimized for small batches (fewer database round trips).
        /// </summary>
        public static BatchInsertConfiguration SmallBatch => new BatchInsertConfiguration
        {
            MaxRowsPerBatch = 100,
            MaxParametersPerStatement = 200,
            EnablePreparedStatementReuse = true,
            EnableMultiRowInsert = true
        };

        /// <summary>
        /// Creates a configuration optimized for large batches (maximum throughput).
        /// </summary>
        public static BatchInsertConfiguration LargeBatch => new BatchInsertConfiguration
        {
            MaxRowsPerBatch = 1000,
            MaxParametersPerStatement = 900,
            EnablePreparedStatementReuse = true,
            EnableMultiRowInsert = true
        };

        /// <summary>
        /// Creates a configuration that disables optimizations (fallback to original behavior).
        /// </summary>
        public static BatchInsertConfiguration Compatible => new BatchInsertConfiguration
        {
            MaxRowsPerBatch = 1,
            MaxParametersPerStatement = 50,
            EnablePreparedStatementReuse = false,
            EnableMultiRowInsert = false
        };

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}