namespace Durable
{
    /// <summary>
    /// Represents an ORDER BY clause for a window function
    /// </summary>
    public class WindowOrderByClause
    {
        /// <summary>
        /// Gets or sets the column name to order by
        /// </summary>
        public string Column { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets a value indicating whether the ordering is ascending (true) or descending (false)
        /// </summary>
        public bool Ascending { get; set; }
    }
}