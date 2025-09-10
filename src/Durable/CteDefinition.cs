namespace Durable
{
    /// <summary>
    /// Represents a Common Table Expression (CTE) definition for SQL queries.
    /// </summary>
    public class CteDefinition
    {
        /// <summary>
        /// Gets or sets the name of the CTE.
        /// </summary>
        public string Name { get; set; } = string.Empty;
        /// <summary>
        /// Gets or sets the SQL query for non-recursive CTEs.
        /// </summary>
        public string Query { get; set; } = string.Empty;
        /// <summary>
        /// Gets or sets a value indicating whether this CTE is recursive.
        /// </summary>
        public bool IsRecursive { get; set; }
        /// <summary>
        /// Gets or sets the anchor query for recursive CTEs.
        /// </summary>
        public string AnchorQuery { get; set; } = string.Empty;
        /// <summary>
        /// Gets or sets the recursive query for recursive CTEs.
        /// </summary>
        public string RecursiveQuery { get; set; } = string.Empty;

        /// <summary>
        /// Initializes a new instance of the CteDefinition class for a non-recursive CTE.
        /// </summary>
        /// <param name="name">The name of the CTE.</param>
        /// <param name="query">The SQL query for the CTE.</param>
        public CteDefinition(string name, string query)
        {
            Name = name;
            Query = query;
            IsRecursive = false;
        }

        /// <summary>
        /// Initializes a new instance of the CteDefinition class for a recursive CTE.
        /// </summary>
        /// <param name="name">The name of the CTE.</param>
        /// <param name="anchorQuery">The anchor query for the recursive CTE.</param>
        /// <param name="recursiveQuery">The recursive query for the recursive CTE.</param>
        public CteDefinition(string name, string anchorQuery, string recursiveQuery)
        {
            Name = name;
            AnchorQuery = anchorQuery;
            RecursiveQuery = recursiveQuery;
            IsRecursive = true;
        }
    }
}