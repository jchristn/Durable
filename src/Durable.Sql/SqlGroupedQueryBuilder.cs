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
    using Durable.Query;

    /// <summary>
    /// Grouped query over a SQL entity query. <see cref="Select{TResult}"/>, <see cref="Count"/> and the aggregates run
    /// as GROUP BY / HAVING statements in the database. <see cref="Execute"/> returns groups with their member entities:
    /// the matching rows are read once and grouped in memory, applying the HAVING predicates to each group.
    /// Thread safety: not thread-safe.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <typeparam name="TKey">Group key type.</typeparam>
    public class SqlGroupedQueryBuilder<T, TKey> : IGroupedQueryBuilder<T, TKey> where T : class, new()
    {
        #region Private-Members

        private readonly SqlQueryBuilder<T> _Source;
        private readonly Expression<Func<T, TKey>> _KeySelector;
        private readonly GroupingSpecification _Grouping;

        #endregion

        #region Constructors-and-Factories

        internal SqlGroupedQueryBuilder(SqlQueryBuilder<T> source, Expression<Func<T, TKey>> keySelector)
        {
            _Source = source;
            _KeySelector = keySelector;
            _Grouping = new GroupingSpecification(keySelector, typeof(IGrouping<TKey, T>));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IGroupedQueryBuilder<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            _Grouping.Having.Add(predicate);
            return this;
        }

        /// <inheritdoc />
        public IQueryBuilder<TResult> Select<TResult>(Expression<Func<IGrouping<TKey, T>, TResult>> selector) where TResult : class, new()
        {
            ArgumentNullException.ThrowIfNull(selector);
            return new SqlProjectionQueryBuilder<T, TResult>(_Source, selector, _Grouping);
        }

        /// <inheritdoc />
        public IEnumerable<IGrouping<TKey, T>> Execute()
        {
            return Group(_Source.Execute());
        }

        /// <inheritdoc />
        public async Task<IEnumerable<IGrouping<TKey, T>>> ExecuteAsync(CancellationToken token = default)
        {
            return Group(await _Source.ExecuteAsync(token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public long Count()
        {
            return Convert.ToInt64(Context.Executor.ExecuteScalar(BuildGroupDerived("COUNT(*)", "1 AS g"), _Source.Transaction, "COUNT") ?? 0L, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildGroupDerived("COUNT(*)", "1 AS g"), _Source.Transaction, "COUNT", token).ConfigureAwait(false);
            return Convert.ToInt64(value ?? 0L, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return ToDecimal(Context.Executor.ExecuteScalar(BuildAggregate("SUM", selector), _Source.Transaction, "SUM"));
        }

        /// <inheritdoc />
        public async Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ToDecimal(await Context.Executor.ExecuteScalarAsync(BuildAggregate("SUM", selector), _Source.Transaction, "SUM", token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return ToDecimal(Context.Executor.ExecuteScalar(BuildAggregate("AVG", selector), _Source.Transaction, "AVG"));
        }

        /// <inheritdoc />
        public async Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ToDecimal(await Context.Executor.ExecuteScalarAsync(BuildAggregate("AVG", selector), _Source.Transaction, "AVG", token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector)
        {
            object? value = Context.Executor.ExecuteScalar(BuildAggregate("MAX", selector), _Source.Transaction, "MAX");
            return value == null ? default! : (TResult)Context.Converter.ConvertFromDatabase(value, typeof(TResult))!;
        }

        /// <inheritdoc />
        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildAggregate("MAX", selector), _Source.Transaction, "MAX", token).ConfigureAwait(false);
            return value == null ? default! : (TResult)Context.Converter.ConvertFromDatabase(value, typeof(TResult))!;
        }

        /// <inheritdoc />
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector)
        {
            object? value = Context.Executor.ExecuteScalar(BuildAggregate("MIN", selector), _Source.Transaction, "MIN");
            return value == null ? default! : (TResult)Context.Converter.ConvertFromDatabase(value, typeof(TResult))!;
        }

        /// <inheritdoc />
        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildAggregate("MIN", selector), _Source.Transaction, "MIN", token).ConfigureAwait(false);
            return value == null ? default! : (TResult)Context.Converter.ConvertFromDatabase(value, typeof(TResult))!;
        }

        #endregion

        #region Private-Methods

        private SqlQueryContext Context => _Source.Context;

        private List<IGrouping<TKey, T>> Group(IEnumerable<T> rows)
        {
            Func<T, TKey> key = _KeySelector.Compile();
            List<IGrouping<TKey, T>> groups = rows.GroupBy(key).ToList();
            foreach (LambdaExpression having in _Grouping.Having)
            {
                Func<IGrouping<TKey, T>, bool> predicate = ((Expression<Func<IGrouping<TKey, T>, bool>>)having).Compile();
                groups = groups.Where(predicate).ToList();
            }

            return groups;
        }

        private SqlStatement BuildGroupDerived(string outerSelect, string innerSelect)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = _Source.BuildModel(builder, false);
            TableSource source = new TableSource(SqlQueryBuilder<T>.RootAlias, _Source.Metadata);
            model.GroupBy.AddRange(Context.CreateTranslator(builder).GroupKeySql(_Grouping, source));
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            translator.UseGrouping(_Grouping, source);
            foreach (LambdaExpression having in _Grouping.Having) model.Having.Add(translator.Predicate(having.Body));
            model.SelectList = innerSelect;
            builder.Append("SELECT ").Append(outerSelect).Append(" FROM (").Append(model.Render(Context.Dialect)).Append(") dq");
            return builder.Build();
        }

        private SqlStatement BuildAggregate<TProperty>(string function, Expression<Func<T, TProperty>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);
            if (_Grouping.Having.Count == 0) return BuildPlainAggregate(function, selector);

            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = _Source.BuildModel(builder, false);
            TableSource source = new TableSource(SqlQueryBuilder<T>.RootAlias, _Source.Metadata);
            model.GroupBy.AddRange(Context.CreateTranslator(builder).GroupKeySql(_Grouping, source));
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            translator.UseGrouping(_Grouping, source);
            foreach (LambdaExpression having in _Grouping.Having) model.Having.Add(translator.Predicate(having.Body));

            SqlExpressionTranslator plain = Context.CreateTranslator(builder);
            plain.Bind(selector.Parameters[0], source);
            string value = plain.Value(selector.Body);
            string outer;
            switch (function)
            {
                case "SUM":
                    model.SelectList = "SUM(" + value + ") AS agg_sum";
                    outer = "SUM(dq.agg_sum)";
                    break;
                case "AVG":
                    model.SelectList = "SUM(CAST(" + value + " AS DECIMAL(38, 10))) AS agg_sum, COUNT(" + value + ") AS agg_count";
                    outer = "SUM(dq.agg_sum) / NULLIF(SUM(dq.agg_count), 0)";
                    break;
                case "MIN":
                    model.SelectList = "MIN(" + value + ") AS agg_value";
                    outer = "MIN(dq.agg_value)";
                    break;
                default:
                    model.SelectList = "MAX(" + value + ") AS agg_value";
                    outer = "MAX(dq.agg_value)";
                    break;
            }

            builder.Append("SELECT ").Append(outer).Append(" FROM (").Append(model.Render(Context.Dialect)).Append(") dq");
            return builder.Build();
        }

        private SqlStatement BuildPlainAggregate<TProperty>(string function, Expression<Func<T, TProperty>> selector)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = _Source.BuildModel(builder, false);
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            translator.Bind(selector.Parameters[0], new TableSource(SqlQueryBuilder<T>.RootAlias, _Source.Metadata));
            string value = translator.Value(selector.Body);
            model.SelectList = function == "AVG" ? "AVG(CAST(" + value + " AS DECIMAL(38, 10)))" : function + "(" + value + ")";
            builder.Append(model.Render(Context.Dialect));
            return builder.Build();
        }

        private static decimal ToDecimal(object? value)
        {
            return value == null ? 0m : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        #endregion
    }
}
