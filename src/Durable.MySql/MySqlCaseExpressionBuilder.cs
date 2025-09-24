namespace Durable.MySql
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a fluent interface for building SQL CASE expressions in MySQL queries.
    /// Allows conditional logic within SQL SELECT statements using WHEN/THEN/ELSE syntax.
    /// </summary>
    /// <typeparam name="TEntity">The entity type that the CASE expression operates on.</typeparam>
    internal class MySqlCaseExpressionBuilder<TEntity> : ICaseExpressionBuilder<TEntity> where TEntity : class, new()
    {
        #region Private-Members

        private readonly MySqlQueryBuilder<TEntity> _QueryBuilder;
        private readonly MySqlRepository<TEntity> _Repository;
        private readonly CaseExpression _CaseExpression;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlCaseExpressionBuilder with the specified query builder and repository.
        /// </summary>
        /// <param name="queryBuilder">The MySQL query builder instance to add the CASE expression to</param>
        /// <param name="repository">The MySQL repository instance used for building WHERE clause conditions</param>
        /// <exception cref="ArgumentNullException">Thrown when queryBuilder or repository is null</exception>
        public MySqlCaseExpressionBuilder(MySqlQueryBuilder<TEntity> queryBuilder, MySqlRepository<TEntity> repository)
        {
            _QueryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _CaseExpression = new CaseExpression();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Adds a WHEN clause to the CASE expression with a condition expressed as a LINQ expression.
        /// </summary>
        /// <param name="condition">A lambda expression defining the condition to evaluate (e.g., x => x.Age > 18)</param>
        /// <param name="result">The value to return when the condition is true. Can be string, number, boolean, or null</param>
        /// <returns>The current builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when condition is null</exception>
        public ICaseExpressionBuilder<TEntity> When(Expression<Func<TEntity, bool>> condition, object result)
        {
            if (condition == null)
                throw new ArgumentNullException(nameof(condition));

            string conditionSql = BuildWhereClause(condition);
            string resultSql = FormatResult(result);
            _CaseExpression.WhenClauses.Add(new WhenClause(conditionSql, resultSql));
            return this;
        }

        /// <summary>
        /// Adds a WHEN clause to the CASE expression with a raw SQL condition string.
        /// </summary>
        /// <param name="condition">The raw SQL condition string to evaluate (e.g., "column_name > 18")</param>
        /// <param name="result">The value to return when the condition is true. Can be string, number, boolean, or null</param>
        /// <returns>The current builder instance for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when condition is null</exception>
        /// <exception cref="ArgumentException">Thrown when condition is empty or whitespace</exception>
        public ICaseExpressionBuilder<TEntity> WhenRaw(string condition, object result)
        {
            if (string.IsNullOrWhiteSpace(condition))
                throw new ArgumentException("Condition cannot be null or empty", nameof(condition));

            string resultSql = FormatResult(result);
            _CaseExpression.WhenClauses.Add(new WhenClause(condition, resultSql));
            return this;
        }

        /// <summary>
        /// Sets the ELSE clause for the CASE expression, defining the default value when no WHEN conditions are met.
        /// </summary>
        /// <param name="result">The default value to return when no WHEN conditions are true. Can be string, number, boolean, or null</param>
        /// <returns>The current builder instance for method chaining</returns>
        public ICaseExpressionBuilder<TEntity> Else(object result)
        {
            _CaseExpression.ElseResult = FormatResult(result);
            return this;
        }

        /// <summary>
        /// Completes the CASE expression construction and adds it to the SELECT clause with the specified alias.
        /// </summary>
        /// <param name="alias">The column alias to use for the CASE expression result in the SELECT clause</param>
        /// <returns>The query builder instance to continue building the query</returns>
        /// <exception cref="ArgumentNullException">Thrown when alias is null</exception>
        /// <exception cref="ArgumentException">Thrown when alias is empty or whitespace</exception>
        public IQueryBuilder<TEntity> EndCase(string alias)
        {
            if (string.IsNullOrWhiteSpace(alias))
                throw new ArgumentException("Alias cannot be null or empty", nameof(alias));

            _CaseExpression.Alias = $"`{alias}`"; // Use MySQL backtick quoting

            // Build the CASE expression SQL with MySQL-specific formatting
            string caseExpressionSql = BuildMySqlCaseExpression();

            // Add the CASE expression to the custom select parts
            // For now, we'll need to implement SelectRaw or similar functionality
            // This is a simplified approach - the actual implementation would need
            // to integrate with the existing SELECT clause building logic
            _QueryBuilder.AddCaseExpression(caseExpressionSql);

            return _QueryBuilder;
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Builds a WHERE clause from a LINQ expression for use in CASE WHEN conditions.
        /// </summary>
        /// <param name="expression">The LINQ expression to convert to SQL</param>
        /// <returns>A SQL WHERE clause string</returns>
        private string BuildWhereClause(Expression<Func<TEntity, bool>> expression)
        {
            // For now, we'll use a simplified approach. In a full implementation,
            // this would use the MySqlExpressionParser to convert the expression to SQL.
            // This is a placeholder implementation.

            if (expression.Body is BinaryExpression binaryExpr)
            {
                return ConvertBinaryExpression(binaryExpr);
            }
            else if (expression.Body is MemberExpression memberExpr)
            {
                return $"`{memberExpr.Member.Name}` = 1"; // For boolean properties
            }

            // Fallback - would need more comprehensive expression parsing
            return "1=1"; // Always true condition as fallback
        }

        /// <summary>
        /// Converts a binary expression to MySQL SQL syntax.
        /// </summary>
        /// <param name="expression">The binary expression to convert</param>
        /// <returns>A SQL condition string</returns>
        private string ConvertBinaryExpression(BinaryExpression expression)
        {
            string left = GetExpressionValue(expression.Left);
            string right = GetExpressionValue(expression.Right);
            string op = GetOperator(expression.NodeType);

            return $"{left} {op} {right}";
        }

        /// <summary>
        /// Gets the SQL representation of an expression value.
        /// </summary>
        /// <param name="expression">The expression to convert</param>
        /// <returns>A SQL value string</returns>
        private string GetExpressionValue(Expression expression)
        {
            if (expression is MemberExpression memberExpr)
            {
                return $"`{memberExpr.Member.Name}`";
            }
            else if (expression is ConstantExpression constExpr)
            {
                return FormatResult(constExpr.Value);
            }

            return "NULL"; // Fallback
        }

        /// <summary>
        /// Converts an expression type to SQL operator.
        /// </summary>
        /// <param name="nodeType">The expression node type</param>
        /// <returns>A SQL operator string</returns>
        private string GetOperator(ExpressionType nodeType)
        {
            return nodeType switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.AndAlso => "AND",
                ExpressionType.OrElse => "OR",
                _ => "="
            };
        }

        /// <summary>
        /// Formats a result value for use in SQL with MySQL-specific formatting.
        /// </summary>
        /// <param name="result">The result value to format</param>
        /// <returns>A SQL-formatted value string</returns>
        private string FormatResult(object? result)
        {
            if (result == null)
            {
                return "NULL";
            }
            else if (result is string stringResult)
            {
                // MySQL string escaping
                return $"'{stringResult.Replace("'", "''").Replace("\\", "\\\\")}'";
            }
            else if (result is bool boolResult)
            {
                return boolResult ? "1" : "0";
            }
            else if (result is DateTime dateTimeResult)
            {
                return $"'{dateTimeResult:yyyy-MM-dd HH:mm:ss}'";
            }
            else
            {
                return result.ToString() ?? "NULL";
            }
        }

        /// <summary>
        /// Builds the MySQL-specific CASE expression SQL.
        /// </summary>
        /// <returns>A complete CASE expression SQL string</returns>
        private string BuildMySqlCaseExpression()
        {
            System.Text.StringBuilder sql = new System.Text.StringBuilder();
            sql.Append("CASE");

            foreach (WhenClause whenClause in _CaseExpression.WhenClauses)
            {
                sql.Append(" WHEN ");
                sql.Append(whenClause.Condition);
                sql.Append(" THEN ");
                sql.Append(whenClause.Result);
            }

            if (!string.IsNullOrEmpty(_CaseExpression.ElseResult))
            {
                sql.Append(" ELSE ");
                sql.Append(_CaseExpression.ElseResult);
            }

            sql.Append(" END");

            if (!string.IsNullOrEmpty(_CaseExpression.Alias))
            {
                sql.Append(" AS ");
                sql.Append(_CaseExpression.Alias);
            }

            return sql.ToString();
        }

        #endregion
    }
}