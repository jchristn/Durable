namespace Durable.Sqlite
{
    using System;
    using System.Linq.Expressions;

    public class SqliteCaseExpressionBuilder<TEntity> : ICaseExpressionBuilder<TEntity> where TEntity : class, new()
    {
        private readonly SqliteQueryBuilder<TEntity> _QueryBuilder;
        private readonly SqliteRepository<TEntity> _Repository;
        private readonly CaseExpression _CaseExpression;

        public SqliteCaseExpressionBuilder(SqliteQueryBuilder<TEntity> queryBuilder, SqliteRepository<TEntity> repository)
        {
            _QueryBuilder = queryBuilder;
            _Repository = repository;
            _CaseExpression = new CaseExpression();
        }

        public ICaseExpressionBuilder<TEntity> When(Expression<Func<TEntity, bool>> condition, object result)
        {
            string conditionSql = _Repository.BuildWhereClause(condition);
            string resultSql = FormatResult(result);
            _CaseExpression.WhenClauses.Add(new WhenClause(conditionSql, resultSql));
            return this;
        }

        public ICaseExpressionBuilder<TEntity> WhenRaw(string condition, object result)
        {
            string resultSql = FormatResult(result);
            _CaseExpression.WhenClauses.Add(new WhenClause(condition, resultSql));
            return this;
        }

        public ICaseExpressionBuilder<TEntity> Else(object result)
        {
            _CaseExpression.ElseResult = FormatResult(result);
            return this;
        }

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