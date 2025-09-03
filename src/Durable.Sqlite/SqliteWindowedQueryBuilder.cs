namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    public class SqliteWindowedQueryBuilder<TEntity> : IWindowedQueryBuilder<TEntity> where TEntity : class, new()
    {
        private readonly SqliteQueryBuilder<TEntity> _QueryBuilder;
        private readonly SqliteRepository<TEntity> _Repository;
        private readonly ITransaction _Transaction;
        private readonly WindowFunction _CurrentWindowFunction;

        public SqliteWindowedQueryBuilder(
            SqliteQueryBuilder<TEntity> queryBuilder,
            SqliteRepository<TEntity> repository,
            ITransaction transaction,
            string functionName,
            string partitionBy,
            string orderBy)
        {
            _QueryBuilder = queryBuilder;
            _Repository = repository;
            _Transaction = transaction;
            _CurrentWindowFunction = new WindowFunction
            {
                FunctionName = functionName
            };
            
            if (!string.IsNullOrEmpty(partitionBy))
            {
                _CurrentWindowFunction.PartitionByColumns.Add(partitionBy);
            }
            
            if (!string.IsNullOrEmpty(orderBy))
            {
                _CurrentWindowFunction.OrderByColumns.Add(new WindowOrderByClause 
                { 
                    Column = orderBy, 
                    Ascending = true 
                });
            }
        }

        public IWindowedQueryBuilder<TEntity> RowNumber(string alias = "row_number")
        {
            _CurrentWindowFunction.FunctionName = "ROW_NUMBER";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Rank(string alias = "rank")
        {
            _CurrentWindowFunction.FunctionName = "RANK";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> DenseRank(string alias = "dense_rank")
        {
            _CurrentWindowFunction.FunctionName = "DENSE_RANK";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Lead<TKey>(Expression<Func<TEntity, TKey>> column, int offset = 1, object defaultValue = null, string alias = "lead")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "LEAD";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["offset"] = offset;
            if (defaultValue != null)
            {
                _CurrentWindowFunction.Parameters["default"] = defaultValue;
            }
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Lag<TKey>(Expression<Func<TEntity, TKey>> column, int offset = 1, object defaultValue = null, string alias = "lag")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "LAG";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["offset"] = offset;
            if (defaultValue != null)
            {
                _CurrentWindowFunction.Parameters["default"] = defaultValue;
            }
            return this;
        }

        public IWindowedQueryBuilder<TEntity> FirstValue<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "first_value")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "FIRST_VALUE";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> LastValue<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "last_value")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "LAST_VALUE";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> NthValue<TKey>(Expression<Func<TEntity, TKey>> column, int n, string alias = "nth_value")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "NTH_VALUE";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["n"] = n;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Sum<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "sum")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "SUM";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Avg<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "avg")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "AVG";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Count(string alias = "count")
        {
            _CurrentWindowFunction.FunctionName = "COUNT";
            _CurrentWindowFunction.Column = "*";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Min<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "min")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "MIN";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Max<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "max")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "MAX";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        public IWindowedQueryBuilder<TEntity> PartitionBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string columnName = _Repository.GetColumnFromExpression(keySelector.Body);
            _CurrentWindowFunction.PartitionByColumns.Add(columnName);
            return this;
        }

        public IWindowedQueryBuilder<TEntity> OrderBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string columnName = _Repository.GetColumnFromExpression(keySelector.Body);
            _CurrentWindowFunction.OrderByColumns.Add(new WindowOrderByClause 
            { 
                Column = columnName, 
                Ascending = true 
            });
            return this;
        }

        public IWindowedQueryBuilder<TEntity> OrderByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string columnName = _Repository.GetColumnFromExpression(keySelector.Body);
            _CurrentWindowFunction.OrderByColumns.Add(new WindowOrderByClause 
            { 
                Column = columnName, 
                Ascending = false 
            });
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Rows(int preceding, int following)
        {
            _CurrentWindowFunction.Frame = new WindowFrame
            {
                Type = WindowFrameType.Rows,
                StartBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.Preceding,
                    Offset = preceding
                },
                EndBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.Following,
                    Offset = following
                }
            };
            return this;
        }

        public IWindowedQueryBuilder<TEntity> RowsUnboundedPreceding()
        {
            if (_CurrentWindowFunction.Frame == null)
            {
                _CurrentWindowFunction.Frame = new WindowFrame
                {
                    Type = WindowFrameType.Rows,
                    StartBound = new WindowFrameBound
                    {
                        Type = WindowFrameBoundType.UnboundedPreceding
                    }
                };
            }
            else
            {
                _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.UnboundedPreceding
                };
            }
            return this;
        }

        public IWindowedQueryBuilder<TEntity> RowsUnboundedFollowing()
        {
            if (_CurrentWindowFunction.Frame == null)
            {
                _CurrentWindowFunction.Frame = new WindowFrame
                {
                    Type = WindowFrameType.Rows,
                    EndBound = new WindowFrameBound
                    {
                        Type = WindowFrameBoundType.UnboundedFollowing
                    }
                };
            }
            else
            {
                _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.UnboundedFollowing
                };
            }
            return this;
        }

        public IWindowedQueryBuilder<TEntity> RowsBetween(string start, string end)
        {
            _CurrentWindowFunction.Frame = new WindowFrame
            {
                Type = WindowFrameType.Rows,
                StartBound = ParseWindowBound(start),
                EndBound = ParseWindowBound(end)
            };
            return this;
        }

        public IWindowedQueryBuilder<TEntity> Range(int preceding, int following)
        {
            _CurrentWindowFunction.Frame = new WindowFrame
            {
                Type = WindowFrameType.Range,
                StartBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.Preceding,
                    Offset = preceding
                },
                EndBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.Following,
                    Offset = following
                }
            };
            return this;
        }

        public IWindowedQueryBuilder<TEntity> RangeUnboundedPreceding()
        {
            if (_CurrentWindowFunction.Frame == null)
            {
                _CurrentWindowFunction.Frame = new WindowFrame
                {
                    Type = WindowFrameType.Range,
                    StartBound = new WindowFrameBound
                    {
                        Type = WindowFrameBoundType.UnboundedPreceding
                    }
                };
            }
            else
            {
                _CurrentWindowFunction.Frame.Type = WindowFrameType.Range;
                _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.UnboundedPreceding
                };
            }
            return this;
        }

        public IWindowedQueryBuilder<TEntity> RangeUnboundedFollowing()
        {
            if (_CurrentWindowFunction.Frame == null)
            {
                _CurrentWindowFunction.Frame = new WindowFrame
                {
                    Type = WindowFrameType.Range,
                    EndBound = new WindowFrameBound
                    {
                        Type = WindowFrameBoundType.UnboundedFollowing
                    }
                };
            }
            else
            {
                _CurrentWindowFunction.Frame.Type = WindowFrameType.Range;
                _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
                {
                    Type = WindowFrameBoundType.UnboundedFollowing
                };
            }
            return this;
        }

        public IWindowedQueryBuilder<TEntity> RangeBetween(string start, string end)
        {
            _CurrentWindowFunction.Frame = new WindowFrame
            {
                Type = WindowFrameType.Range,
                StartBound = ParseWindowBound(start),
                EndBound = ParseWindowBound(end)
            };
            return this;
        }

        public IQueryBuilder<TEntity> EndWindow()
        {
            // Add the window function to the query builder
            _QueryBuilder._WindowFunctions.Add(_CurrentWindowFunction);
            return _QueryBuilder;
        }

        public IEnumerable<TEntity> Execute()
        {
            EndWindow();
            return _QueryBuilder.Execute();
        }

        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            EndWindow();
            return await _QueryBuilder.ExecuteAsync(token);
        }

        public IAsyncEnumerable<TEntity> ExecuteAsyncEnumerable(CancellationToken token = default)
        {
            EndWindow();
            return _QueryBuilder.ExecuteAsyncEnumerable(token);
        }

        private WindowFrameBound ParseWindowBound(string boundString)
        {
            boundString = boundString.ToUpper();
            
            if (boundString == "UNBOUNDED PRECEDING")
            {
                return new WindowFrameBound { Type = WindowFrameBoundType.UnboundedPreceding };
            }
            else if (boundString == "UNBOUNDED FOLLOWING")
            {
                return new WindowFrameBound { Type = WindowFrameBoundType.UnboundedFollowing };
            }
            else if (boundString == "CURRENT ROW")
            {
                return new WindowFrameBound { Type = WindowFrameBoundType.CurrentRow };
            }
            else if (boundString.EndsWith(" PRECEDING"))
            {
                string offsetStr = boundString.Replace(" PRECEDING", "");
                if (int.TryParse(offsetStr, out int offset))
                {
                    return new WindowFrameBound 
                    { 
                        Type = WindowFrameBoundType.Preceding, 
                        Offset = offset 
                    };
                }
            }
            else if (boundString.EndsWith(" FOLLOWING"))
            {
                string offsetStr = boundString.Replace(" FOLLOWING", "");
                if (int.TryParse(offsetStr, out int offset))
                {
                    return new WindowFrameBound 
                    { 
                        Type = WindowFrameBoundType.Following, 
                        Offset = offset 
                    };
                }
            }
            
            return new WindowFrameBound { Type = WindowFrameBoundType.CurrentRow };
        }
    }
}