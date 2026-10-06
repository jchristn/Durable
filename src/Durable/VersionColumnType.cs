namespace Durable
{
    /// <summary>
    /// How a <see cref="VersionColumnAttribute"/> column's value changes on each update. Durable generates every new
    /// value itself (client side); no version type relies on a database-generated value such as SQL Server's
    /// <c>rowversion</c>.
    /// </summary>
    public enum VersionColumnType
    {
        /// <summary>
        /// An 8-byte big-endian counter stored in a <c>byte[]</c> property (a binary/blob column). New rows start at
        /// 1 and each entity update increments it; set-based updates (UpdateField, BatchUpdate) assign a fresh value derived
        /// from the current UTC time so held copies become stale. This is not SQL Server's server-generated
        /// <c>rowversion</c>/<c>timestamp</c> type: map it to an ordinary binary column.
        /// </summary>
        BinaryCounter,

        /// <summary>
        /// The UTC time of the last write, in a <see cref="System.DateTime"/> property.
        /// </summary>
        Timestamp,

        /// <summary>
        /// An integer counter in an <c>int</c>, <c>long</c>, <c>short</c> or <c>byte</c> property: new rows start at 1
        /// and every update adds 1 (set-based updates increment in the database).
        /// </summary>
        Integer,

        /// <summary>
        /// A new <see cref="System.Guid"/> on every write, in a <c>Guid</c> property.
        /// </summary>
        Guid
    }
}
