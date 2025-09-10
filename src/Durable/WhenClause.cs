namespace Durable
{
    /// <summary>
    /// Represents a WHEN clause in a CASE expression, containing a condition and result.
    /// </summary>
    public class WhenClause
    {
        /// <summary>
        /// Gets or sets the condition to evaluate.
        /// </summary>
        public string Condition { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets the result to return when the condition is true.
        /// </summary>
        public string Result { get; set; } = string.Empty;

        /// <summary>
        /// Initializes a new instance of the <see cref="WhenClause"/> class.
        /// </summary>
        /// <param name="condition">The condition to evaluate.</param>
        /// <param name="result">The result to return when the condition is true.</param>
        public WhenClause(string condition, string result)
        {
            Condition = condition;
            Result = result;
        }
    }
}