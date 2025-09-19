namespace Durable.Sqlite
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a fluent interface for building SQL CASE expressions in SQLite queries.
    /// Allows conditional logic within SQL SELECT statements using WHEN/THEN/ELSE syntax.
    /// </summary>
    /// <typeparam name="TEntity">The entity type that the CASE expression operates on.</typeparam>
    public class SqliteCaseExpressionBuilder<TEntity> : ICaseExpressionBuilder<TEntity> where TEntity : class, new()
    {
        private readonly SqliteQueryBuilder<TEntity> _QueryBuilder;
        private readonly SqliteRepository<TEntity> _Repository;
        private readonly CaseExpression _CaseExpression;

        /// <summary>
        /// Initializes a new instance of the SqliteCaseExpressionBuilder with the specified query builder and repository.
        /// </summary>
        /// <param name="queryBuilder">The SQLite query builder instance to add the CASE expression to.</param>
        /// <param name="repository">The SQLite repository instance used for building WHERE clause conditions.</param>
        /// <exception cref="ArgumentNullException">Thrown when queryBuilder or repository is null.</exception>
        public SqliteCaseExpressionBuilder(SqliteQueryBuilder<TEntity> queryBuilder, SqliteRepository<TEntity> repository)
        {
            _QueryBuilder = queryBuilder;
            _Repository = repository;
            _CaseExpression = new CaseExpression();
        }

        /// <summary>
        /// Adds a WHEN clause to the CASE expression with a condition expressed as a LINQ expression.
        /// </summary>
        /// <param name="condition">A lambda expression defining the condition to evaluate (e.g., x => x.Age > 18).</param>
        /// <param name="result">The value to return when the condition is true. Can be string, number, boolean, or null.</param>
        /// <returns>The current builder instance for method chaining.</returns>
        public ICaseExpressionBuilder<TEntity> When(Expression<Func<TEntity, bool>> condition, object result)
        {
            string conditionSql = _Repository.BuildWhereClause(condition);
            string resultSql = FormatResult(result);
            _CaseExpression.WhenClauses.Add(new WhenClause(conditionSql, resultSql));
            return this;
        }

        /// <summary>
        /// Adds a WHEN clause to the CASE expression with a raw SQL condition string.
        /// </summary>
        /// <param name="condition">The raw SQL condition string to evaluate (e.g., "column_name > 18").</param>
        /// <param name="result">The value to return when the condition is true. Can be string, number, boolean, or null.</param>
        /// <returns>The current builder instance for method chaining.</returns>
        public ICaseExpressionBuilder<TEntity> WhenRaw(string condition, object result)
        {
            string resultSql = FormatResult(result);
            _CaseExpression.WhenClauses.Add(new WhenClause(condition, resultSql));
            return this;
        }

        /// <summary>
        /// Sets the ELSE clause for the CASE expression, defining the default value when no WHEN conditions are met.
        /// </summary>
        /// <param name="result">The default value to return when no WHEN conditions are true. Can be string, number, boolean, or null.</param>
        /// <returns>The current builder instance for method chaining.</returns>
        public ICaseExpressionBuilder<TEntity> Else(object result)
        {
            _CaseExpression.ElseResult = FormatResult(result);
            return this;
        }

        /// <summary>
        /// Completes the CASE expression construction and adds it to the SELECT clause with the specified alias.
        /// </summary>
        /// <param name="alias">The column alias to use for the CASE expression result in the SELECT clause.</param>
        /// <returns>The query builder instance to continue building the query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when alias is null.</exception>
        /// <exception cref="ArgumentException">Thrown when alias is empty or whitespace.</exception>
        public IQueryBuilder<TEntity> EndCase(string alias)
        {
            _CaseExpression.Alias = alias;
            
            // Add the CASE expression to the custom select parts
            string currentSelect = _QueryBuilder.GetCustomSelectClause() ?? "t0.*";
            string caseExpressionSql = _CaseExpression.BuildSql();
            string newSelect = currentSelect + ", " + caseExpressionSql;
            _QueryBuilder.SelectRaw(newSelect);
            
            return _QueryBuilder;
        }

        private string FormatResult(object result)
        {
            if (result == null)
            {
                return "NULL";
            }
            else if (result is string stringResult)
            {
                return $"'{stringResult.Replace("'", "''")}'";
            }
            else if (result is bool boolResult)
            {
                return boolResult ? "1" : "0";
            }
            else
            {
                return result.ToString();
            }
        }
    }
}