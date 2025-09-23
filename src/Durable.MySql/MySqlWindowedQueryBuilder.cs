namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides window function support for MySQL queries, enabling advanced analytical operations like ROW_NUMBER, RANK, and aggregate functions with window clauses.
    /// Supports MySQL's full window function syntax including frames, partitioning, and ordering.
    /// </summary>
    /// <typeparam name="TEntity">The entity type for the query. Must be a class with a parameterless constructor.</typeparam>
    internal class MySqlWindowedQueryBuilder<TEntity> : IWindowedQueryBuilder<TEntity> where TEntity : class, new()
    {
        #region Private-Members

        private readonly MySqlQueryBuilder<TEntity> _QueryBuilder;
        private readonly MySqlRepository<TEntity> _Repository;
        private readonly ITransaction? _Transaction;
        private readonly WindowFunction _CurrentWindowFunction;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlWindowedQueryBuilder class.
        /// </summary>
        /// <param name="queryBuilder">The base query builder to extend with window functions</param>
        /// <param name="repository">The repository instance for database operations</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <param name="functionName">The name of the window function to use</param>
        /// <param name="partitionBy">Optional PARTITION BY clause for the window function</param>
        /// <param name="orderBy">Optional ORDER BY clause for the window function</param>
        /// <exception cref="ArgumentNullException">Thrown when queryBuilder or repository is null</exception>
        public MySqlWindowedQueryBuilder(
            MySqlQueryBuilder<TEntity> queryBuilder,
            MySqlRepository<TEntity> repository,
            ITransaction? transaction,
            string functionName,
            string? partitionBy,
            string? orderBy)
        {
            _QueryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
            _Repository = repository ?? throw new ArgumentNullException(nameof(repository));
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

        #endregion

        #region Public-Methods

        /// <summary>
        /// Adds a ROW_NUMBER() window function to the query.
        /// </summary>
        /// <param name="alias">The alias for the row number column</param>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> RowNumber(string alias = "row_number")
        {
            _CurrentWindowFunction.FunctionName = "ROW_NUMBER";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a RANK() window function to the query.
        /// </summary>
        /// <param name="alias">The alias for the rank column</param>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> Rank(string alias = "rank")
        {
            _CurrentWindowFunction.FunctionName = "RANK";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a DENSE_RANK() window function to the query.
        /// </summary>
        /// <param name="alias">The alias for the dense rank column</param>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> DenseRank(string alias = "dense_rank")
        {
            _CurrentWindowFunction.FunctionName = "DENSE_RANK";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a LEAD() window function to access data from a subsequent row.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to access from the subsequent row</param>
        /// <param name="offset">The number of rows to look ahead</param>
        /// <param name="defaultValue">The default value to return if no subsequent row is found</param>
        /// <param name="alias">The alias for the lead column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> Lead<TKey>(Expression<Func<TEntity, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lead")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "LEAD";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["offset"] = offset;
            if (defaultValue != null)
                _CurrentWindowFunction.Parameters["default"] = defaultValue;
            return this;
        }

        /// <summary>
        /// Adds a LAG() window function to access data from a previous row.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to access from the previous row</param>
        /// <param name="offset">The number of rows to look back</param>
        /// <param name="defaultValue">The default value to return if no previous row is found</param>
        /// <param name="alias">The alias for the lag column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> Lag<TKey>(Expression<Func<TEntity, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lag")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "LAG";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["offset"] = offset;
            if (defaultValue != null)
                _CurrentWindowFunction.Parameters["default"] = defaultValue;
            return this;
        }

        /// <summary>
        /// Adds a FIRST_VALUE() window function to get the first value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to get the first value from</param>
        /// <param name="alias">The alias for the first value column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> FirstValue<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "first_value")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "FIRST_VALUE";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a LAST_VALUE() window function to get the last value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to get the last value from</param>
        /// <param name="alias">The alias for the last value column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> LastValue<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "last_value")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "LAST_VALUE";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a NTH_VALUE() window function to get the nth value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to get the nth value from</param>
        /// <param name="n">The position of the value to retrieve (1-based)</param>
        /// <param name="alias">The alias for the nth value column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when n is less than 1</exception>
        public IWindowedQueryBuilder<TEntity> NthValue<TKey>(Expression<Func<TEntity, TKey>> column, int n, string alias = "nth_value")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));
            if (n < 1)
                throw new ArgumentOutOfRangeException(nameof(n), "N must be at least 1");

            _CurrentWindowFunction.FunctionName = "NTH_VALUE";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            _CurrentWindowFunction.Parameters["n"] = n;
            return this;
        }

        /// <summary>
        /// Adds a SUM() window function to calculate the sum of values in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to sum</param>
        /// <param name="alias">The alias for the sum column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> Sum<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "sum")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "SUM";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds an AVG() window function to calculate the average of values in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to average</param>
        /// <param name="alias">The alias for the average column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> Avg<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "avg")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "AVG";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a COUNT() window function to count rows in the window frame.
        /// </summary>
        /// <param name="alias">The alias for the count column</param>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> Count(string alias = "count")
        {
            _CurrentWindowFunction.FunctionName = "COUNT";
            _CurrentWindowFunction.Column = "*";
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a MIN() window function to find the minimum value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to find the minimum value from</param>
        /// <param name="alias">The alias for the minimum column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> Min<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "min")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "MIN";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Adds a MAX() window function to find the maximum value in the window frame.
        /// </summary>
        /// <typeparam name="TKey">The type of the column value</typeparam>
        /// <param name="column">The column to find the maximum value from</param>
        /// <param name="alias">The alias for the maximum column</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        public IWindowedQueryBuilder<TEntity> Max<TKey>(Expression<Func<TEntity, TKey>> column, string alias = "max")
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            _CurrentWindowFunction.FunctionName = "MAX";
            _CurrentWindowFunction.Column = ExtractColumnName(column);
            _CurrentWindowFunction.Alias = alias;
            return this;
        }

        /// <summary>
        /// Partitions the result set by the specified key selector for the window function.
        /// </summary>
        /// <typeparam name="TKey">The type of the partition key</typeparam>
        /// <param name="keySelector">The expression to partition by</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        public IWindowedQueryBuilder<TEntity> PartitionBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            string columnName = ExtractColumnName(keySelector);
            if (!_CurrentWindowFunction.PartitionByColumns.Contains(columnName))
            {
                _CurrentWindowFunction.PartitionByColumns.Add(columnName);
            }
            return this;
        }

        /// <summary>
        /// Orders the window frame by the specified key selector in ascending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the order key</typeparam>
        /// <param name="keySelector">The expression to order by</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        public IWindowedQueryBuilder<TEntity> OrderBy<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            string columnName = ExtractColumnName(keySelector);
            _CurrentWindowFunction.OrderByColumns.Add(new WindowOrderByClause
            {
                Column = columnName,
                Ascending = true
            });
            return this;
        }

        /// <summary>
        /// Orders the window frame by the specified key selector in descending order.
        /// </summary>
        /// <typeparam name="TKey">The type of the order key</typeparam>
        /// <param name="keySelector">The expression to order by</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when keySelector is null</exception>
        public IWindowedQueryBuilder<TEntity> OrderByDescending<TKey>(Expression<Func<TEntity, TKey>> keySelector)
        {
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));

            string columnName = ExtractColumnName(keySelector);
            _CurrentWindowFunction.OrderByColumns.Add(new WindowOrderByClause
            {
                Column = columnName,
                Ascending = false
            });
            return this;
        }

        /// <summary>
        /// Specifies a ROWS window frame with the given preceding and following row counts.
        /// </summary>
        /// <param name="preceding">The number of preceding rows to include</param>
        /// <param name="following">The number of following rows to include</param>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> Rows(int preceding, int following)
        {
            _CurrentWindowFunction.Frame.Type = WindowFrameType.Rows;
            _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.Preceding,
                Offset = preceding
            };
            _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.Following,
                Offset = following
            };
            return this;
        }

        /// <summary>
        /// Specifies a ROWS window frame from unbounded preceding to current row.
        /// </summary>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> RowsUnboundedPreceding()
        {
            _CurrentWindowFunction.Frame.Type = WindowFrameType.Rows;
            _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.UnboundedPreceding
            };
            _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.CurrentRow
            };
            return this;
        }

        /// <summary>
        /// Specifies a ROWS window frame from current row to unbounded following.
        /// </summary>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> RowsUnboundedFollowing()
        {
            _CurrentWindowFunction.Frame.Type = WindowFrameType.Rows;
            _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.CurrentRow
            };
            _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.UnboundedFollowing
            };
            return this;
        }

        /// <summary>
        /// Specifies a ROWS window frame between the given start and end boundaries.
        /// </summary>
        /// <param name="start">The start boundary of the window frame</param>
        /// <param name="end">The end boundary of the window frame</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when start or end is null</exception>
        public IWindowedQueryBuilder<TEntity> RowsBetween(string start, string end)
        {
            if (string.IsNullOrEmpty(start))
                throw new ArgumentNullException(nameof(start));
            if (string.IsNullOrEmpty(end))
                throw new ArgumentNullException(nameof(end));

            _CurrentWindowFunction.Frame.Type = WindowFrameType.Rows;
            _CurrentWindowFunction.Frame.StartBound = ParseWindowBound(start);
            _CurrentWindowFunction.Frame.EndBound = ParseWindowBound(end);
            return this;
        }

        /// <summary>
        /// Specifies a RANGE window frame with the given preceding and following value ranges.
        /// </summary>
        /// <param name="preceding">The preceding value range to include</param>
        /// <param name="following">The following value range to include</param>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> Range(int preceding, int following)
        {
            _CurrentWindowFunction.Frame.Type = WindowFrameType.Range;
            _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.Preceding,
                Offset = preceding
            };
            _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.Following,
                Offset = following
            };
            return this;
        }

        /// <summary>
        /// Specifies a RANGE window frame from unbounded preceding to current row.
        /// </summary>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> RangeUnboundedPreceding()
        {
            _CurrentWindowFunction.Frame.Type = WindowFrameType.Range;
            _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.UnboundedPreceding
            };
            _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.CurrentRow
            };
            return this;
        }

        /// <summary>
        /// Specifies a RANGE window frame from current row to unbounded following.
        /// </summary>
        /// <returns>The current query builder for method chaining</returns>
        public IWindowedQueryBuilder<TEntity> RangeUnboundedFollowing()
        {
            _CurrentWindowFunction.Frame.Type = WindowFrameType.Range;
            _CurrentWindowFunction.Frame.StartBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.CurrentRow
            };
            _CurrentWindowFunction.Frame.EndBound = new WindowFrameBound
            {
                Type = WindowFrameBoundType.UnboundedFollowing
            };
            return this;
        }

        /// <summary>
        /// Specifies a RANGE window frame between the given start and end boundaries.
        /// </summary>
        /// <param name="start">The start boundary of the window frame</param>
        /// <param name="end">The end boundary of the window frame</param>
        /// <returns>The current query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when start or end is null</exception>
        public IWindowedQueryBuilder<TEntity> RangeBetween(string start, string end)
        {
            if (string.IsNullOrEmpty(start))
                throw new ArgumentNullException(nameof(start));
            if (string.IsNullOrEmpty(end))
                throw new ArgumentNullException(nameof(end));

            _CurrentWindowFunction.Frame.Type = WindowFrameType.Range;
            _CurrentWindowFunction.Frame.StartBound = ParseWindowBound(start);
            _CurrentWindowFunction.Frame.EndBound = ParseWindowBound(end);
            return this;
        }

        /// <summary>
        /// Ends the window function configuration and returns to the regular query builder.
        /// </summary>
        /// <returns>The regular query builder for continued query construction</returns>
        public IQueryBuilder<TEntity> EndWindow()
        {
            _QueryBuilder.AddWindowFunction(_CurrentWindowFunction);
            return _QueryBuilder;
        }

        /// <summary>
        /// Executes the windowed query synchronously and returns the results.
        /// </summary>
        /// <returns>An enumerable collection of entities with window function results</returns>
        public IEnumerable<TEntity> Execute()
        {
            EndWindow();
            return _QueryBuilder.Execute();
        }

        /// <summary>
        /// Executes the windowed query asynchronously and returns the results.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <returns>A task that represents the asynchronous operation containing an enumerable collection of entities with window function results</returns>
        public async Task<IEnumerable<TEntity>> ExecuteAsync(CancellationToken token = default)
        {
            EndWindow();
            return await _QueryBuilder.ExecuteAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the windowed query asynchronously and returns the results as an async enumerable for streaming.
        /// </summary>
        /// <param name="token">Cancellation token to cancel the operation</param>
        /// <returns>An async enumerable collection of entities with window function results that can be consumed with await foreach</returns>
        public IAsyncEnumerable<TEntity> ExecuteAsyncEnumerable(CancellationToken token = default)
        {
            EndWindow();
            return _QueryBuilder.ExecuteAsyncEnumerable(token);
        }

        #endregion

        #region Private-Methods

        private string ExtractColumnName<TKey>(Expression<Func<TEntity, TKey>> expression)
        {
            if (expression?.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }
            throw new ArgumentException("Expression must be a member access expression", nameof(expression));
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
                string valueStr = boundString.Substring(0, boundString.Length - " PRECEDING".Length);
                if (int.TryParse(valueStr, out int value))
                {
                    return new WindowFrameBound { Type = WindowFrameBoundType.Preceding, Offset = value };
                }
            }
            else if (boundString.EndsWith(" FOLLOWING"))
            {
                string valueStr = boundString.Substring(0, boundString.Length - " FOLLOWING".Length);
                if (int.TryParse(valueStr, out int value))
                {
                    return new WindowFrameBound { Type = WindowFrameBoundType.Following, Offset = value };
                }
            }

            throw new ArgumentException($"Invalid window frame bound: {boundString}", nameof(boundString));
        }

        #endregion
    }
}