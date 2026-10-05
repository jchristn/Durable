namespace Durable.Sql
{
    using System;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Renders statements into a reviewable SQL script: parameters are inlined as literals through the dialect, each
    /// statement is terminated, and the dialect's batch separator (for example GO) is written after it.
    /// Thread safety: stateless.
    /// </summary>
    internal static class MigrationScriptWriter
    {
        #region Public-Methods

        internal static string Inline(ISqlDialect dialect, SqlStatement statement)
        {
            string sql = statement.Sql;
            foreach (SqlParameterValue parameter in statement.Parameters.OrderByDescending(p => p.Name.Length))
            {
                string literal = dialect.FormatLiteral(parameter.Value);
                sql = Regex.Replace(sql, "(?<![\\w@])" + Regex.Escape(parameter.Name) + "(?!\\w)", _ => literal);
            }

            return sql;
        }

        internal static void AppendStatement(StringBuilder script, ISqlDialect dialect, SqlStatement statement)
        {
            string sql = Inline(dialect, statement).TrimEnd();
            script.Append(sql);
            if (!sql.EndsWith(dialect.StatementSeparator, StringComparison.Ordinal)) script.Append(dialect.StatementSeparator);
            script.AppendLine();
            if (dialect.ScriptBatchSeparator != null) script.AppendLine(dialect.ScriptBatchSeparator);
        }

        internal static void AppendComment(StringBuilder script, string text)
        {
            foreach (string line in text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'))
                script.Append("-- ").AppendLine(line);
        }

        #endregion
    }
}
