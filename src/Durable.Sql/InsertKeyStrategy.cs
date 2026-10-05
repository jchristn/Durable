namespace Durable.Sql
{
    /// <summary>
    /// How a dialect returns database-generated values from an INSERT.
    /// </summary>
    public enum InsertKeyStrategy
    {
        /// <summary>
        /// <c>INSERT ... RETURNING cols</c> (SQLite 3.35+, PostgreSQL).
        /// </summary>
        Returning = 0,

        /// <summary>
        /// <c>INSERT ... OUTPUT INSERTED.cols VALUES ...</c> (SQL Server).
        /// </summary>
        Output = 1,

        /// <summary>
        /// <c>INSERT ...; SELECT LAST_INSERT_ID()</c> (MySQL).
        /// </summary>
        LastInsertId = 2
    }
}
