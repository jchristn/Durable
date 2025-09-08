namespace Durable
{
    using System.Collections.Generic;
    using System.Text;

    public class CaseExpression
    {
        public List<WhenClause> WhenClauses { get; set; }
        public string ElseResult { get; set; } = string.Empty;
        public string Alias { get; set; } = string.Empty;

        public CaseExpression()
        {
            WhenClauses = new List<WhenClause>();
        }

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