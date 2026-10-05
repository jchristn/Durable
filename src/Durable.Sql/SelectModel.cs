namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// The rendered parts of a SELECT statement. Query builders translate their state into a model (adding parameters to
    /// the shared <see cref="SqlStatementBuilder"/>), then the model renders the final text in dialect order.
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class SelectModel
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the CTE definitions (each "name AS (sql)"). Never null.
        /// </summary>
        public List<string> Ctes { get; } = new List<string>();

        /// <summary>
        /// Gets or sets whether any CTE is recursive.
        /// </summary>
        public bool RecursiveCte { get; set; }

        /// <summary>
        /// Gets or sets whether SELECT DISTINCT is used.
        /// </summary>
        public bool Distinct { get; set; }

        /// <summary>
        /// Gets or sets the select list. Default: "t0.*".
        /// </summary>
        public string SelectList { get; set; } = "t0.*";

        /// <summary>
        /// Gets or sets the FROM source including its alias. Must be set before rendering.
        /// </summary>
        public string From { get; set; } = string.Empty;

        /// <summary>
        /// Gets the JOIN clauses. Never null.
        /// </summary>
        public List<string> Joins { get; } = new List<string>();

        /// <summary>
        /// Gets the WHERE conditions, combined with AND. Never null.
        /// </summary>
        public List<string> Conditions { get; } = new List<string>();

        /// <summary>
        /// Gets the GROUP BY expressions. Never null.
        /// </summary>
        public List<string> GroupBy { get; } = new List<string>();

        /// <summary>
        /// Gets the HAVING conditions, combined with AND. Never null.
        /// </summary>
        public List<string> Having { get; } = new List<string>();

        /// <summary>
        /// Gets the ORDER BY items (expression plus direction). Never null.
        /// </summary>
        public List<string> OrderBy { get; } = new List<string>();

        /// <summary>
        /// Gets the set operations applied to the core SELECT (operator keyword plus core SQL). Never null.
        /// </summary>
        public List<KeyValuePair<string, string>> SetOperations { get; } = new List<KeyValuePair<string, string>>();

        /// <summary>
        /// Gets or sets rows to skip.
        /// </summary>
        public int? Skip { get; set; }

        /// <summary>
        /// Gets or sets rows to take.
        /// </summary>
        public int? Take { get; set; }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Renders the core SELECT (no CTEs, ordering, paging or set operations).
        /// </summary>
        /// <returns>SQL text.</returns>
        public string RenderCore()
        {
            StringBuilder sb = new StringBuilder(256);
            sb.Append("SELECT ");
            if (Distinct) sb.Append("DISTINCT ");
            sb.Append(SelectList).Append(" FROM ").Append(From);
            foreach (string join in Joins) sb.Append(' ').Append(join);
            if (Conditions.Count > 0) sb.Append(" WHERE ").Append(string.Join(" AND ", Conditions));
            if (GroupBy.Count > 0) sb.Append(" GROUP BY ").Append(string.Join(", ", GroupBy));
            if (Having.Count > 0) sb.Append(" HAVING ").Append(string.Join(" AND ", Having));
            return sb.ToString();
        }

        /// <summary>
        /// Renders the complete statement.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <returns>SQL text.</returns>
        /// <exception cref="ArgumentNullException">Thrown when dialect is null.</exception>
        public string Render(ISqlDialect dialect)
        {
            ArgumentNullException.ThrowIfNull(dialect);
            bool empty = Take.HasValue && Take.Value == 0;
            if (empty && SetOperations.Count == 0) Conditions.Add("(1 = 0)");
            SqlStatementBuilder text = new SqlStatementBuilder(dialect);
            if (Ctes.Count > 0)
                text.Append(RecursiveCte ? dialect.RecursiveCteKeyword : "WITH").Append(" ").Append(string.Join(", ", Ctes)).Append(" ");

            if (SetOperations.Count > 0)
            {
                text.Append("SELECT * FROM (").Append(RenderCore());
                foreach (KeyValuePair<string, string> operation in SetOperations)
                    text.Append(" ").Append(operation.Key).Append(" ").Append(operation.Value);
                text.Append(") t0");
                if (empty) text.Append(" WHERE 1 = 0");
            }
            else
            {
                text.Append(RenderCore());
            }

            if (OrderBy.Count > 0) text.Append(" ORDER BY ").Append(string.Join(", ", OrderBy));
            if (!empty && (Skip.HasValue || Take.HasValue)) dialect.AppendPaging(text, Skip, Take, OrderBy.Count > 0);
            return text.Sql.ToString();
        }

        #endregion
    }
}
