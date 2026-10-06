namespace Durable.Sql
{
    /// <summary>
    /// Batch insert settings read by SQL repositories (<see cref="SqlRepositoryOptions.BatchConfiguration"/>) for
    /// <c>CreateMany</c>. <see cref="BatchInsertConfiguration"/> is the standard implementation.
    /// </summary>
    public interface IBatchInsertConfiguration
    {
        /// <summary>
        /// Gets the maximum number of rows per INSERT command (multi-row statement or batch of single-row statements).
        /// Minimum: 1.
        /// </summary>
        int MaxRowsPerBatch { get; }

        /// <summary>
        /// Gets the maximum number of parameters per INSERT command. The effective limit never exceeds the dialect's
        /// <see cref="ISqlDialect.MaxParameters"/>. Minimum: 1.
        /// </summary>
        int MaxParametersPerStatement { get; }

        /// <summary>
        /// Gets whether to use multi-row INSERT syntax for entities without a generated key. When false, each row gets its
        /// own INSERT statement (the statements of one chunk are still sent as one command).
        /// </summary>
        bool EnableMultiRowInsert { get; }
    }
}