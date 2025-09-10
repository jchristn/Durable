namespace Durable
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents a SQL window function with its configuration and parameters
    /// </summary>
    public class WindowFunction
    {
        /// <summary>
        /// Gets or sets the name of the window function (e.g., ROW_NUMBER, RANK, SUM)
        /// </summary>
        public string FunctionName { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets the column name the function operates on
        /// </summary>
        public string Column { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets the alias for the window function result
        /// </summary>
        public string Alias { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets the list of columns to partition the data by
        /// </summary>
        public List<string> PartitionByColumns { get; set; }
        
        /// <summary>
        /// Gets or sets the list of order by clauses for the window function
        /// </summary>
        public List<WindowOrderByClause> OrderByColumns { get; set; }
        
        /// <summary>
        /// Gets or sets the window frame specification
        /// </summary>
        public WindowFrame Frame { get; set; } = new WindowFrame();
        
        /// <summary>
        /// Gets or sets additional parameters for the window function
        /// </summary>
        public Dictionary<string, object> Parameters { get; set; }

        /// <summary>
        /// Initializes a new instance of the WindowFunction class
        /// </summary>
        public WindowFunction()
        {
            PartitionByColumns = new List<string>();
            OrderByColumns = new List<WindowOrderByClause>();
            Parameters = new Dictionary<string, object>();
        }
    }
}