namespace Durable.MySql
{
    /// <summary>
    /// The MySQL-compatible database a repository talks to. Each flavor selects a dialect
    /// (<see cref="MySqlDialect.For(MySqlFlavor)"/>) that adjusts the SQL Durable generates for that database.
    /// </summary>
    public enum MySqlFlavor
    {
        /// <summary>
        /// MySQL (default). Uses <see cref="MySqlDialect"/>.
        /// </summary>
        MySql = 0,

        /// <summary>
        /// MariaDB. Uses <see cref="MariaDbDialect"/>.
        /// </summary>
        MariaDb = 1
    }
}
