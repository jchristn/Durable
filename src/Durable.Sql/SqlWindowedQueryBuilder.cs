namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Defines one window (PARTITION BY, ORDER BY, frame) and the functions computed over it; <see cref="EndWindow"/>
    /// adds them to the parent query's select list. Function arguments and default values are parameterized; aliases and
    /// frame bounds are validated.
    /// Thread safety: not thread-safe.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class SqlWindowedQueryBuilder<T> : IWindowedQueryBuilder<T> where T : class, new()
    {
        #region Private-Members

        private readonly SqlQueryBuilder<T> _Parent;
        private readonly string _DefaultFunction;
        private readonly string? _PartitionRaw;
        private readonly string? _OrderRaw;
        private readonly List<LambdaExpression> _Partitions = new List<LambdaExpression>();
        private readonly List<KeyValuePair<LambdaExpression, bool>> _Orderings = new List<KeyValuePair<LambdaExpression, bool>>();
        private readonly List<Func<SqlExpressionTranslator, TableSource, string>> _Functions = new List<Func<SqlExpressionTranslator, TableSource, string>>();
        private string? _Frame;
        private bool _Ended;

        #endregion

        #region Constructors-and-Factories

        internal SqlWindowedQueryBuilder(SqlQueryBuilder<T> parent, string functionName, string? partitionBy, string? orderBy)
        {
            _Parent = parent;
            _DefaultFunction = SqlIdentifierValidator.RequireIdentifier(functionName, nameof(functionName)).ToUpperInvariant();
            _PartitionRaw = partitionBy;
            _OrderRaw = orderBy;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RowNumber(string alias = "row_number") => AddFunction("ROW_NUMBER", null, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Rank(string alias = "rank") => AddFunction("RANK", null, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> DenseRank(string alias = "dense_rank") => AddFunction("DENSE_RANK", null, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Lead<TKey>(Expression<Func<T, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lead")
        {
            return AddOffsetFunction("LEAD", column, offset, defaultValue, alias);
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Lag<TKey>(Expression<Func<T, TKey>> column, int offset = 1, object? defaultValue = null, string alias = "lag")
        {
            return AddOffsetFunction("LAG", column, offset, defaultValue, alias);
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> FirstValue<TKey>(Expression<Func<T, TKey>> column, string alias = "first_value") => AddFunction("FIRST_VALUE", column, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> LastValue<TKey>(Expression<Func<T, TKey>> column, string alias = "last_value") => AddFunction("LAST_VALUE", column, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> NthValue<TKey>(Expression<Func<T, TKey>> column, int n, string alias = "nth_value")
        {
            ArgumentNullException.ThrowIfNull(column);
            if (n < 1) throw new ArgumentOutOfRangeException(nameof(n), "n must be at least 1.");
            string quotedAlias = QuoteAlias(alias);
            _Functions.Add((translator, source) =>
            {
                translator.Bind(column.Parameters[0], source);
                return "NTH_VALUE(" + translator.Value(column.Body) + ", " + n.ToString(CultureInfo.InvariantCulture) + ")" + Over(translator, source) + " AS " + quotedAlias;
            });
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Sum<TKey>(Expression<Func<T, TKey>> column, string alias = "sum") => AddFunction("SUM", column, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Avg<TKey>(Expression<Func<T, TKey>> column, string alias = "avg") => AddFunction("AVG", column, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Count(string alias = "count")
        {
            string quotedAlias = QuoteAlias(alias);
            _Functions.Add((translator, source) => "COUNT(*)" + Over(translator, source) + " AS " + quotedAlias);
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Min<TKey>(Expression<Func<T, TKey>> column, string alias = "min") => AddFunction("MIN", column, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Max<TKey>(Expression<Func<T, TKey>> column, string alias = "max") => AddFunction("MAX", column, alias);

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> PartitionBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Partitions.Add(keySelector);
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Rows(int preceding, int following)
        {
            if (preceding < 0 || following < 0) throw new ArgumentOutOfRangeException(nameof(preceding), "Frame offsets cannot be negative.");
            _Frame = "ROWS BETWEEN " + preceding.ToString(CultureInfo.InvariantCulture) + " PRECEDING AND " + following.ToString(CultureInfo.InvariantCulture) + " FOLLOWING";
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RowsUnboundedPreceding()
        {
            _Frame = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RowsUnboundedFollowing()
        {
            _Frame = "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING";
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RowsBetween(string start, string end)
        {
            _Frame = "ROWS BETWEEN " + SqlIdentifierValidator.RequireFrameBound(start, nameof(start)) + " AND " + SqlIdentifierValidator.RequireFrameBound(end, nameof(end));
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> Range(int preceding, int following)
        {
            if (preceding < 0 || following < 0) throw new ArgumentOutOfRangeException(nameof(preceding), "Frame offsets cannot be negative.");
            _Frame = "RANGE BETWEEN " + preceding.ToString(CultureInfo.InvariantCulture) + " PRECEDING AND " + following.ToString(CultureInfo.InvariantCulture) + " FOLLOWING";
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RangeUnboundedPreceding()
        {
            _Frame = "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RangeUnboundedFollowing()
        {
            _Frame = "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING";
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> RangeBetween(string start, string end)
        {
            _Frame = "RANGE BETWEEN " + SqlIdentifierValidator.RequireFrameBound(start, nameof(start)) + " AND " + SqlIdentifierValidator.RequireFrameBound(end, nameof(end));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> EndWindow()
        {
            if (!_Ended)
            {
                _Ended = true;
                if (_Functions.Count == 0) AddFunction(_DefaultFunction, null, _DefaultFunction.ToLowerInvariant());
                foreach (Func<SqlExpressionTranslator, TableSource, string> function in _Functions) _Parent.AddSelectItem(function);
            }

            return _Parent;
        }

        /// <inheritdoc />
        public IEnumerable<T> Execute() => EndWindow().Execute();

        /// <inheritdoc />
        public Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default) => EndWindow().ExecuteAsync(token);

        /// <inheritdoc />
        public IAsyncEnumerable<T> ExecuteAsyncEnumerable(CancellationToken token = default) => EndWindow().ExecuteAsyncEnumerable(token);

        #endregion

        #region Private-Methods

        private IWindowedQueryBuilder<T> AddFunction(string function, LambdaExpression? column, string alias)
        {
            string quotedAlias = QuoteAlias(alias);
            _Functions.Add((translator, source) =>
            {
                string argument = "";
                if (column != null)
                {
                    translator.Bind(column.Parameters[0], source);
                    argument = translator.Value(column.Body);
                }

                return function + "(" + argument + ")" + Over(translator, source) + " AS " + quotedAlias;
            });
            return this;
        }

        private IWindowedQueryBuilder<T> AddOffsetFunction(string function, LambdaExpression column, int offset, object? defaultValue, string alias)
        {
            ArgumentNullException.ThrowIfNull(column);
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Offset cannot be negative.");
            string quotedAlias = QuoteAlias(alias);
            _Functions.Add((translator, source) =>
            {
                translator.Bind(column.Parameters[0], source);
                string value = translator.Value(column.Body);
                ColumnReference? reference = translator.ResolveColumn(column.Body);
                string arguments = value + ", " + offset.ToString(CultureInfo.InvariantCulture);
                if (defaultValue != null) arguments += ", " + translator.Parameter(defaultValue, reference?.Column);
                return function + "(" + arguments + ")" + Over(translator, source) + " AS " + quotedAlias;
            });
            return this;
        }

        private string Over(SqlExpressionTranslator translator, TableSource source)
        {
            List<string> parts = new List<string>();
            List<string> partitions = new List<string>();
            if (!string.IsNullOrWhiteSpace(_PartitionRaw)) partitions.Add(_PartitionRaw!);
            foreach (LambdaExpression partition in _Partitions)
            {
                translator.Bind(partition.Parameters[0], source);
                partitions.Add(translator.Value(partition.Body));
            }

            if (partitions.Count > 0) parts.Add("PARTITION BY " + string.Join(", ", partitions));

            List<string> orderings = new List<string>();
            if (!string.IsNullOrWhiteSpace(_OrderRaw)) orderings.Add(_OrderRaw!);
            foreach (KeyValuePair<LambdaExpression, bool> ordering in _Orderings)
            {
                translator.Bind(ordering.Key.Parameters[0], source);
                orderings.Add(translator.Value(ordering.Key.Body) + translator.Dialect.OrderDirection(ordering.Value));
            }

            if (orderings.Count > 0) parts.Add("ORDER BY " + string.Join(", ", orderings));
            if (_Frame != null) parts.Add(_Frame);
            return " OVER (" + string.Join(" ", parts) + ")";
        }

        private string QuoteAlias(string alias)
        {
            return _Parent.Context.Dialect.QuoteIdentifier(SqlIdentifierValidator.RequireIdentifier(alias, nameof(alias)));
        }

        #endregion
    }
}
