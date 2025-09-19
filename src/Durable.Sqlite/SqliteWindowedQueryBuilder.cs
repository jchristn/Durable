namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides window function support for SQLite queries, enabling advanced analytical operations like ROW_NUMBER, RANK, and aggregate functions with window clauses.
    /// </summary>
    /// <typeparam name="TEntity">The entity type for the query. Must be a class with a parameterless constructor.</typeparam>
    public class SqliteWindowedQueryBuilder<TEntity> : IWindowedQueryBuilder<TEntity> where TEntity : class, new()
    {
        private readonly SqliteQueryBuilder<TEntity> _QueryBuilder;
        private readonly SqliteRepository<TEntity> _Repository;
        private readonly ITransaction _Transaction;
        private readonly WindowFunction _CurrentWindowFunction;

        /// <summary>
        /// Initializes a new instance of the SqliteWindowedQueryBuilder class.
        /// </summary>
        /// <param name="queryBuilder">The base query builder to extend with window functions.</param>
        /// <param name="repository">The repository instance for database operations.</param>
        /// <param name="transaction">Optional transaction to execute within.</param>
        /// <param name="functionName">The name of the window function to use.</param>
        /// <param name="partitionBy">Optional PARTITION BY clause for the window function.</param>
        /// <param name="orderBy">Optional ORDER BY clause for the window function.</param>
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

        /// <summary>
        /// Adds a ROW_NUMBER() window function to assign sequential numbers to rows within the window partition.
        /// </summary>
        /// <param name="alias">The alias for the row number column in the result set. Default is "row_number".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> RowNumber(string alias = "row_number")
        {
            _CurrentWindowFunction.FunctionName = "ROW_NUMBER";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a RANK() window function to assign ranks to rows within the window partition, with gaps for tied values.
        /// </summary>
        /// <param name="alias">The alias for the rank column in the result set. Default is "rank".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> Rank(string alias = "rank")
        {
            _CurrentWindowFunction.FunctionName = "RANK";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a DENSE_RANK() window function to assign ranks to rows within the window partition, without gaps for tied values.
        /// </summary>
        /// <param name="alias">The alias for the dense rank column in the result set. Default is "dense_rank".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> DenseRank(string alias = "dense_rank")
        {
            _CurrentWindowFunction.FunctionName = "DENSE_RANK";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a LEAD() window function to access data from subsequent rows within the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being accessed.</typeparam>
        /// <param name="column">Expression that selects the column to access from subsequent rows.</param>
        /// <param name="offset">The number of rows forward to look. Default is 1.</param>
        /// <param name="defaultValue">Default value to return when no subsequent row exists. Default is null.</param>
        /// <param name="alias">The alias for the lead column in the result set. Default is "lead".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Adds a LAG() window function to access data from preceding rows within the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being accessed.</typeparam>
        /// <param name="column">Expression that selects the column to access from preceding rows.</param>
        /// <param name="offset">The number of rows backward to look. Default is 1.</param>
        /// <param name="defaultValue">Default value to return when no preceding row exists. Default is null.</param>
        /// <param name="alias">The alias for the lag column in the result set. Default is "lag".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Adds a FIRST_VALUE() window function to return the first value in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being accessed.</typeparam>
        /// <param name="column">Expression that selects the column to get the first value from.</param>
        /// <param name="alias">The alias for the first value column in the result set. Default is "first_value".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> FirstValue<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "first_value")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "FIRST_VALUE";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a LAST_VALUE() window function to return the last value in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being accessed.</typeparam>
        /// <param name="column">Expression that selects the column to get the last value from.</param>
        /// <param name="alias">The alias for the last value column in the result set. Default is "last_value".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> LastValue<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "last_value")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "LAST_VALUE";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds an NTH_VALUE() window function to return the nth value in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being accessed.</typeparam>
        /// <param name="column">Expression that selects the column to get the nth value from.</param>
        /// <param name="n">The position (1-based) of the value to return.</param>
        /// <param name="alias">The alias for the nth value column in the result set. Default is "nth_value".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> NthValue<TKey>(Expression<Func<TEntity, TKey>> column, int n, string alias = "nth_value")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "NTH_VALUE";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["n"] = n;
            return this;
        }

        /// <summary>
        /// Adds a SUM() window function to calculate the sum of values in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being summed.</typeparam>
        /// <param name="column">Expression that selects the column to sum.</param>
        /// <param name="alias">The alias for the sum column in the result set. Default is "sum".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> Sum<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "sum")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "SUM";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds an AVG() window function to calculate the average of values in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being averaged.</typeparam>
        /// <param name="column">Expression that selects the column to average.</param>
        /// <param name="alias">The alias for the average column in the result set. Default is "avg".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> Avg<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "avg")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "AVG";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a COUNT() window function to count rows in the window partition.
        /// </summary>
        /// <param name="alias">The alias for the count column in the result set. Default is "count".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> Count(string alias = "count")
        {
            _CurrentWindowFunction.FunctionName = "COUNT";
            _CurrentWindowFunction.Column = "*";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a MIN() window function to find the minimum value in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being evaluated.</typeparam>
        /// <param name="column">Expression that selects the column to find the minimum of.</param>
        /// <param name="alias">The alias for the minimum column in the result set. Default is "min".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> Min<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "min")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "MIN";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a MAX() window function to find the maximum value in the window partition.
        /// </summary>
        /// <typeparam name="TKey">The type of the column being evaluated.</typeparam>
        /// <param name="column">Expression that selects the column to find the maximum of.</param>
        /// <param name="alias">The alias for the maximum column in the result set. Default is "max".</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> Max<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "max")
        {
            string columnName = _Repository.GetColumnFromExpression(column.Body);
            _CurrentWindowFunction.FunctionName = "MAX";
            _CurrentWindowFunction.Column = columnName;
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a PARTITION BY clause to the window function, dividing rows into partitions.
        /// </summary>
        /// <typeparam name="TKey">The type of the partition key.</typeparam>
        /// <param name="keySelector">Expression that selects the column to partition by.</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
        public IWindowedQueryBuilder<TEntity> PartitionBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            string columnName = _Repository.GetColumnFromExpression(keySelector.Body);
            _CurrentWindowFunction.PartitionByColumns.Add(columnName);
            return this;
        }

        /// <summary>
        /// Adds an ORDER BY clause to the window function in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">Expression that selects the column to order by.</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Adds an ORDER BY clause to the window function in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the ordering key.</typeparam>
        /// <param name="keySelector">Expression that selects the column to order by in descending order.</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Defines a window frame using ROWS with specific preceding and following offsets.
        /// </summary>
        /// <param name="preceding">Number of rows preceding the current row to include in the frame.</param>
        /// <param name="following">Number of rows following the current row to include in the frame.</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Sets the window frame to include all rows from the start of the partition up to the current row.
        /// </summary>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Sets the window frame to include all rows from the current row to the end of the partition.
        /// </summary>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Defines a custom window frame using ROWS BETWEEN with string-based boundary specifications.
        /// </summary>
        /// <param name="start">The start boundary specification (e.g., "UNBOUNDED PRECEDING", "2 PRECEDING").</param>
        /// <param name="end">The end boundary specification (e.g., "CURRENT ROW", "1 FOLLOWING").</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Defines a window frame using RANGE with specific preceding and following value offsets.
        /// </summary>
        /// <param name="preceding">Value offset preceding the current row to include in the frame.</param>
        /// <param name="following">Value offset following the current row to include in the frame.</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Sets the window frame to use RANGE with unbounded preceding, including all preceding values.
        /// </summary>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Sets the window frame to use RANGE with unbounded following, including all following values.
        /// </summary>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Defines a custom window frame using RANGE BETWEEN with string-based boundary specifications.
        /// </summary>
        /// <param name="start">The start boundary specification (e.g., "UNBOUNDED PRECEDING", "2 PRECEDING").</param>
        /// <param name="end">The end boundary specification (e.g., "CURRENT ROW", "1 FOLLOWING").</param>
        /// <returns>The windowed query builder instance for method chaining.</returns>
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

        /// <summary>
        /// Completes the window function configuration and returns the base query builder for further operations.
        /// </summary>
        /// <returns>The base query builder instance with the window function applied.</returns>
        public IQueryBuilder<TEntity> EndWindow()
        {
            // Add the window function to the query builder
            _QueryBuilder._WindowFunctions.Add(_CurrentWindowFunction);
            return _QueryBuilder;
        }

        /// <summary>
        /// Executes the windowed query synchronously and returns the results.
        /// </summary>
        /// <returns>An enumerable collection of entities with window function results.</returns>
        public IEnumerable<TEntity> Execute()
        {
            EndWindow();
            return _QueryBuilder.Execute();
        }

        /// <summary>
        /// Executes the windowed query asynchronously and returns the results.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing an enumerable collection of entities with window function results.</returns>
        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            EndWindow();
            return await _QueryBuilder.ExecuteAsync(token);
        }

        /// <summary>
        /// Executes the windowed query asynchronously and returns the results as an async enumerable for streaming.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation.</param>
        /// <returns>An async enumerable collection of entities with window function results that can be consumed with await foreach.</returns>
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