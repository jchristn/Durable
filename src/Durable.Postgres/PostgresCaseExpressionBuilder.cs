namespace Durable.Postgres
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a fluent interface for building SQL CASE expressions in PostgreSQL queries.
    /// Allows conditional logic within SQL SELECT statements using WHEN/THEN/ELSE syntax.
    /// </summary>
    /// <typeparam name="TEntity">The entity type that the CASE expression operates on.</typeparam>
    internal class PostgresCaseExpressionBuilder<TEntity> : ICaseExpressionBuilder<TEntity> where TEntity : class, new()
    {

        #region Private-Members

        private readonly PostgresQueryBuilder<TEntity> _QueryBuilder;
        private readonly PostgresRepository<TEntity> _Repository;
        private readonly CaseExpression _CaseExpression;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresCaseExpressionBuilder with the specified query builder and repository.
        /// </summary>
        /// <param name="queryBuilder">The PostgreSQL query builder instance to add the CASE expression to</param>
        /// <param name="repository">The PostgreSQL repository instance used for building WHERE clause conditions</param>
        /// <exception cref="ArgumentNullException">Thrown when queryBuilder or repository is null</exception>
        public PostgresCaseExpressionBuilder(PostgresQueryBuilder<TEntity> queryBuilder, PostgresRepository<TEntity> repository)
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

            _CaseExpression.Alias = _Repository._Sanitizer.SanitizeIdentifier(alias);

            // Build the CASE expression SQL with PostgreSQL-specific formatting
            string caseExpressionSql = BuildPostgresCaseExpression();

            // Add the CASE expression to the query builder
            _QueryBuilder.AddCaseExpression(caseExpressionSql);

            return _QueryBuilder;
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Builds a WHERE clause from a LINQ expression for use in CASE WHEN conditions.
        /// Uses the full PostgresExpressionParser to support complex expressions, method calls,
        /// navigation properties, and all standard LINQ query patterns.
        /// </summary>
        /// <param name="expression">The LINQ expression to convert to SQL</param>
        /// <returns>A SQL WHERE clause string</returns>
        private string BuildWhereClause(Expression<Func<TEntity, bool>> expression)
        {
            // Use the full PostgresExpressionParser for comprehensive expression support
            PostgresExpressionParser<TEntity> parser = new PostgresExpressionParser<TEntity>(
                _Repository._ColumnMappings,
                _Repository._Sanitizer
            );

            // Parse the expression without parameters (embedded values for CASE expressions)
            return parser.ParseExpression(expression.Body);
        }

        /// <summary>
        /// Formats a result value for use in SQL with PostgreSQL-specific formatting.
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
                // Use the sanitizer for proper PostgreSQL string escaping
                return _Repository._Sanitizer.FormatValue(stringResult);
            }
            else if (result is bool boolResult)
            {
                return boolResult ? "true" : "false";
            }
            else if (result is DateTime dateTimeResult)
            {
                // PostgreSQL timestamp format
                return $"'{dateTimeResult:yyyy-MM-dd HH:mm:ss}'::timestamp";
            }
            else
            {
                return _Repository._Sanitizer.FormatValue(result);
            }
        }

        /// <summary>
        /// Builds the PostgreSQL-specific CASE expression SQL.
        /// </summary>
        /// <returns>A complete CASE expression SQL string</returns>
        private string BuildPostgresCaseExpression()
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