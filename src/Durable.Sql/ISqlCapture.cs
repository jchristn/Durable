namespace Durable.Sql
{
    /// <summary>
    /// Exposes the last SQL statement a repository executed, for debugging and logging. Every SQL repository implements
    /// it. Capture is per async flow: concurrent operations on one repository each see their own last statement. To get
    /// the SQL of a single call without turning capture on, use the <c>*WithQuery</c> extensions or
    /// <see cref="SqlCaptureScope"/>.
    /// </summary>
    public interface ISqlCapture
    {
        /// <summary>
        /// Gets the last SQL statement that was executed by this repository instance.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        string? LastExecutedSql { get; }

        /// <summary>
        /// Gets the last SQL statement with parameter values substituted that was executed by this repository instance.
        /// This provides a fully executable SQL statement with actual parameter values for debugging purposes.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        string? LastExecutedSqlWithParameters { get; }

        /// <summary>
        /// Gets or sets whether executed SQL is captured for <see cref="LastExecutedSql"/>. Default: false (initialized from
        /// <see cref="SqlRepositoryOptions.CaptureSql"/>).
        /// </summary>
        bool CaptureSql { get; set; }

    }
}