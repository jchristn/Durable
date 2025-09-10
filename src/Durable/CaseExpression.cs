namespace Durable
{
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Represents a SQL CASE expression that can be used to build conditional SQL statements.
    /// </summary>
    public class CaseExpression
    {
        /// <summary>
        /// Gets or sets the list of WHEN clauses that define the conditions and their corresponding results.
        /// </summary>
        public List<WhenClause> WhenClauses { get; set; }
        
        /// <summary>
        /// Gets or sets the ELSE result that will be used when none of the WHEN conditions are met.
        /// </summary>
        public string ElseResult { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets the alias to be used for the CASE expression in the SQL statement.
        /// </summary>
        public string Alias { get; set; } = string.Empty;

        /// <summary>
        /// Initializes a new instance of the <see cref="CaseExpression"/> class.
        /// </summary>
        public CaseExpression()
        {
            WhenClauses = new List<WhenClause>();
        }

        /// <summary>
        /// Builds and returns the SQL representation of the CASE expression.
        /// </summary>
        /// <returns>A string containing the complete SQL CASE expression.</returns>
        public string BuildSql()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("CASE");
            
            foreach (WhenClause whenClause in WhenClauses)
            {
                sql.Append(" WHEN ");
                sql.Append(whenClause.Condition);
                sql.Append(" THEN ");
                sql.Append(whenClause.Result);
            }
            
            if (!string.IsNullOrEmpty(ElseResult))
            {
                sql.Append(" ELSE ");
                sql.Append(ElseResult);
            }
            
            sql.Append(" END");
            
            if (!string.IsNullOrEmpty(Alias))
            {
                sql.Append(" AS ");
                sql.Append(Alias);
            }
            
            return sql.ToString();
        }
    }
}