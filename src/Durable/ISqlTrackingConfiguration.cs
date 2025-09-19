namespace Durable
{
    /// <summary>
    /// Provides configuration options for SQL tracking behavior on repository instances.
    /// This interface is optional and allows repositories to expose per-instance SQL tracking settings.
    /// </summary>
    public interface ISqlTrackingConfiguration
    {

        #region Public-Members

        /// <summary>
        /// Gets or sets whether query results should automatically include the executed SQL statement.
        /// When true, repository operations will return IDurableResult objects containing both results and SQL.
        /// When false, repository operations return standard result types without SQL information.
        /// Default value is false for performance and backward compatibility.
        /// </summary>
        bool IncludeQueryInResults { get; set; }

        #endregion

    }
}