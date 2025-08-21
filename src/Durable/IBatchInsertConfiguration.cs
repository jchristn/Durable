namespace Durable
{
    /// <summary>
    /// Configuration interface for batch insert optimizations.
    /// Repository implementations can optionally implement this to customize batch behavior.
    /// </summary>
    public interface IBatchInsertConfiguration
    {
        /// <summary>
        /// Gets the maximum number of rows to include in a single multi-row INSERT statement.
        /// Default is typically 500-1000 depending on the database provider.
        /// </summary>
        int MaxRowsPerBatch { get; }

        /// <summary>
        /// Gets the maximum number of parameters per INSERT statement.
        /// Some databases have limits (e.g., SQLite has a default limit of 999 parameters).
        /// </summary>
        int MaxParametersPerStatement { get; }

        /// <summary>
        /// Gets whether to use prepared statement reuse for batch operations.
        /// When true, the same prepared statement is reused across batches.
        /// </summary>
        bool EnablePreparedStatementReuse { get; }

        /// <summary>
        /// Gets whether to use multi-row INSERT syntax when possible.
        /// When false, falls back to individual INSERT statements in a transaction.
        /// </summary>
        bool EnableMultiRowInsert { get; }
    }
}