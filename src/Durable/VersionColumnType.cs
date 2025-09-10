namespace Durable
{
    /// <summary>
    /// Specifies the type of version column for concurrency control
    /// </summary>
    public enum VersionColumnType
    {
        /// <summary>
        /// Binary row version (typically used with SQL Server ROWVERSION)
        /// </summary>
        RowVersion,
        /// <summary>
        /// Timestamp-based versioning using DateTime
        /// </summary>
        Timestamp,
        /// <summary>
        /// Integer-based versioning (incremental counter)
        /// </summary>
        Integer,
        /// <summary>
        /// GUID-based versioning using unique identifiers
        /// </summary>
        Guid
    }
}