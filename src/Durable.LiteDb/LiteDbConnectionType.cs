namespace Durable.LiteDb
{
    /// <summary>
    /// How a file-backed LiteDB database is opened.
    /// </summary>
    public enum LiteDbConnectionType
    {
        /// <summary>
        /// The process opens the data file exclusively and keeps it open (fastest; one process per file). Default.
        /// </summary>
        Direct = 0,

        /// <summary>
        /// The data file is opened and closed around every operation under a named system mutex, so several processes can
        /// share one file. Slower than <see cref="Direct"/>.
        /// </summary>
        Shared = 1
    }
}
