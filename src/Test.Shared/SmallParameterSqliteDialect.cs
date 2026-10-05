namespace Test.Shared
{
    using Durable.Sqlite;

    /// <summary>
    /// SQLite dialect with an artificially small parameter limit, used to force Include IN-list chunking with few rows.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public class SmallParameterSqliteDialect : SqliteDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the maximum parameters per statement. Always 60, which yields Include chunks of 10 keys.
        /// </summary>
        public override int MaxParameters => 60;

        #endregion
    }
}
