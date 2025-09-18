namespace Durable
{
    /// <summary>
    /// Provides the ability to capture and expose the last executed SQL statement.
    /// This interface is optional and can be implemented by repositories that support SQL tracking.
    /// </summary>
    public interface ISqlCapture
    {

        #region Public-Members

        /// <summary>
        /// Gets the last SQL statement that was executed by this repository instance.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        string LastExecutedSql { get; }

        /// <summary>
        /// Gets the last SQL statement with parameter values substituted that was executed by this repository instance.
        /// This provides a fully executable SQL statement with actual parameter values for debugging purposes.
        /// Returns null if no SQL has been executed or SQL capture is disabled.
        /// </summary>
        string LastExecutedSqlWithParameters { get; }

        /// <summary>
        /// Gets or sets whether SQL statements should be captured and stored.
        /// Default value is false for performance reasons.
        /// </summary>
        bool CaptureSql { get; set; }

        #endregion

    }
}