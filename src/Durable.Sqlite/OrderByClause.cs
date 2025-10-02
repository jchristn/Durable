namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents an ORDER BY clause in a SQL query with column and direction information.
    /// </summary>
    internal class OrderByClause
    {
        /// <summary>
        /// Gets or sets the column name to order by.
        /// </summary>
        public string Column { get; set; }

        /// <summary>
        /// Gets or sets whether to sort in ascending order (true) or descending order (false).
        /// </summary>
        public bool Ascending { get; set; }
    }
}
