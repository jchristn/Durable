namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// A query producing a projected type computed in SQL, from <c>Select</c> on an entity query or on a grouped query.
    /// Predicates and orderings over the projected type are rewritten to the source expressions (WHERE for plain projections,
    /// HAVING for grouped projections). Includes, grouping, deletes, set operations and raw fragments other than
    /// <see cref="WhereRaw"/> are not supported on projections.
    /// Thread safety: not thread-safe.
    /// </summary>
    /// <typeparam name="TSource">Source entity type.</typeparam>
    /// <typeparam name="TResult">Projected type.</typeparam>
    public class SqlProjectionQueryBuilder<TSource, TResult> : ISqlQueryBuilder<TResult>
        where TSource : class, new()
        where TResult : class, new()
    {
        #region Public-Members

        /// <inheritdoc />
        public string Query => BuildSql();

        #endregion

        #region Private-Members

        private readonly SqlQueryBuilder<TSource> _Source;
        private readonly ParameterExpression _SelectorParameter;
        private readonly List<KeyValuePair<string, Expression>> _Bindings = new List<KeyValuePair<string, Expression>>();
        private readonly Dictionary<string, Expression> _BindingsByName = new Dictionary<string, Expression>(StringComparer.Ordinal);
        private readonly GroupingSpecification? _Grouping;
        private readonly List<Func<SqlExpressionTranslator, string>> _Conditions = new List<Func<SqlExpressionTranslator, string>>();
        private readonly List<KeyValuePair<LambdaExpression, bool>> _Orderings = new List<KeyValuePair<LambdaExpression, bool>>();
        private readonly EntityMetadata _ResultMetadata;
        private int? _Skip;
        private int? _Take;
        private bool _Distinct;

        #endregion

        #region Constructors-and-Factories

        internal SqlProjectionQueryBuilder(SqlQueryBuilder<TSource> source, LambdaExpression selector, GroupingSpecification? grouping = null)
        {
            _Source = source;
            _Grouping = grouping;
            _SelectorParameter = selector.Parameters[0];
            _ResultMetadata = EntityMetadata.For(typeof(TResult));

            Expression body = selector.Body;
            if (body is MemberInitExpression memberInit)
            {
                foreach (MemberBinding binding in memberInit.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        throw new NotSupportedException("Projection bindings must be simple assignments.");
                    _Bindings.Add(new KeyValuePair<string, Expression>(assignment.Member.Name, assignment.Expression));
                    _BindingsByName[assignment.Member.Name] = assignment.Expression;
                }
            }
            else if (body is NewExpression newExpression && newExpression.Arguments.Count == 0)
            {
            }
            else
            {
                throw new NotSupportedException("Select must use an object initializer, for example x => new Summary { Name = x.Name }.");
            }
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> Where(Expression<Func<TResult, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            Expression rewritten = Rewrite(predicate);
            _Conditions.Add(translator => translator.Predicate(rewritten));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> ThenBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> ThenByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new KeyValuePair<LambdaExpression, bool>(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> Skip(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Skip count cannot be negative.");
            _Skip = count;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> Take(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Take count cannot be negative.");
            _Take = count;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> Distinct()
        {
            _Distinct = true;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> IgnoreQueryFilters()
        {
            _Source.IgnoreQueryFilters();
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> WhereRaw(string sql, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            object?[] values = parameters ?? Array.Empty<object?>();
            _Conditions.Add(translator => "(" + RawSql.BindPlaceholders(sql, values, translator.Builder, translator.Converter) + ")");
            return this;
        }

        /// <inheritdoc />
        public IEnumerable<TResult> Execute()
        {
            return Context.Executor.Query(BuildStatement(), _Source.Transaction, "SELECT", CreateMapper()).ToList();
        }

        /// <inheritdoc />
        public async Task<IEnumerable<TResult>> ExecuteAsync(CancellationToken token = default)
        {
            List<TResult> results = new List<TResult>();
            await foreach (TResult item in ExecuteAsyncEnumerable(token).ConfigureAwait(false)) results.Add(item);
            return results;
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<TResult> ExecuteAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            await foreach (TResult item in Context.Executor.QueryAsync(BuildStatement(), _Source.Transaction, "SELECT", CreateMapper(), token).ConfigureAwait(false))
                yield return item;
        }

        /// <inheritdoc />
        public IDurableResult<TResult> ExecuteWithQuery()
        {
            return new DurableResult<TResult>(BuildSql(), Execute());
        }

        /// <inheritdoc />
        public async Task<IDurableResult<TResult>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string sql = BuildSql();
            return new DurableResult<TResult>(sql, await ExecuteAsync(token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public IAsyncDurableResult<TResult> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            return new AsyncDurableResult<TResult>(BuildSql(), ExecuteAsyncEnumerable(token));
        }

        /// <inheritdoc />
        public long Count()
        {
            return Convert.ToInt64(Context.Executor.ExecuteScalar(BuildDerived("COUNT(*)"), _Source.Transaction, "COUNT") ?? 0L, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildDerived("COUNT(*)"), _Source.Transaction, "COUNT", token).ConfigureAwait(false);
            return Convert.ToInt64(value ?? 0L, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public bool Any()
        {
            return Count() > 0;
        }

        /// <inheritdoc />
        public async Task<bool> AnyAsync(CancellationToken token = default)
        {
            return await CountAsync(token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            return ToDecimal(Context.Executor.ExecuteScalar(BuildDerived("SUM(dq." + Label(selector) + ")"), _Source.Transaction, "SUM"));
        }

        /// <inheritdoc />
        public async Task<decimal> SumAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            return ToDecimal(await Context.Executor.ExecuteScalarAsync(BuildDerived("SUM(dq." + Label(selector) + ")"), _Source.Transaction, "SUM", token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            return ToDecimal(Context.Executor.ExecuteScalar(BuildDerived("AVG(CAST(dq." + Label(selector) + " AS DECIMAL(38, 10)))"), _Source.Transaction, "AVG"));
        }

        /// <inheritdoc />
        public async Task<decimal> AverageAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            return ToDecimal(await Context.Executor.ExecuteScalarAsync(BuildDerived("AVG(CAST(dq." + Label(selector) + " AS DECIMAL(38, 10)))"), _Source.Transaction, "AVG", token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public TProperty Min<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            object? value = Context.Executor.ExecuteScalar(BuildDerived("MIN(dq." + Label(selector) + ")"), _Source.Transaction, "MIN");
            return value == null ? default! : (TProperty)Context.Converter.ConvertFromDatabase(value, typeof(TProperty))!;
        }

        /// <inheritdoc />
        public async Task<TProperty> MinAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildDerived("MIN(dq." + Label(selector) + ")"), _Source.Transaction, "MIN", token).ConfigureAwait(false);
            return value == null ? default! : (TProperty)Context.Converter.ConvertFromDatabase(value, typeof(TProperty))!;
        }

        /// <inheritdoc />
        public TProperty Max<TProperty>(Expression<Func<TResult, TProperty>> selector)
        {
            object? value = Context.Executor.ExecuteScalar(BuildDerived("MAX(dq." + Label(selector) + ")"), _Source.Transaction, "MAX");
            return value == null ? default! : (TProperty)Context.Converter.ConvertFromDatabase(value, typeof(TProperty))!;
        }

        /// <inheritdoc />
        public async Task<TProperty> MaxAsync<TProperty>(Expression<Func<TResult, TProperty>> selector, CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildDerived("MAX(dq." + Label(selector) + ")"), _Source.Transaction, "MAX", token).ConfigureAwait(false);
            return value == null ? default! : (TProperty)Context.Converter.ConvertFromDatabase(value, typeof(TProperty))!;
        }

        /// <inheritdoc />
        public SqlStatement BuildStatement()
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            builder.Append(BuildModel(builder, true).Render(Context.Dialect));
            return builder.Build();
        }

        /// <inheritdoc />
        public string BuildSql()
        {
            return BuildStatement().Sql;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return BuildSql();
        }

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> Include<TProperty>(Expression<Func<TResult, TProperty>> navigationProperty) => throw NotSupported("Include");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty) => throw NotSupported("ThenInclude");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TOther> Select<TOther>(Expression<Func<TResult, TOther>> selector) where TOther : class, new() => throw NotSupported("Select");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public IGroupedQueryBuilder<TResult, TKey> GroupBy<TKey>(Expression<Func<TResult, TKey>> keySelector) => throw NotSupported("GroupBy");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> Union(IQueryBuilder<TResult> other) => throw NotSupported("Union");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> UnionAll(IQueryBuilder<TResult> other) => throw NotSupported("UnionAll");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> Intersect(IQueryBuilder<TResult> other) => throw NotSupported("Intersect");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> Except(IQueryBuilder<TResult> other) => throw NotSupported("Except");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WhereIn<TKey, TOther>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey) where TOther : class, new() => throw NotSupported("WhereIn");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WhereNotIn<TKey, TOther>(Expression<Func<TResult, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey) where TOther : class, new() => throw NotSupported("WhereNotIn");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WhereInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string subquerySql, params object?[] parameters) => throw NotSupported("WhereInRaw");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WhereNotInRaw<TKey>(Expression<Func<TResult, TKey>> keySelector, string subquerySql, params object?[] parameters) => throw NotSupported("WhereNotInRaw");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WhereExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<TResult, TOther, bool>>? correlation = null) where TOther : class, new() => throw NotSupported("WhereExists");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<TResult, TOther, bool>>? correlation = null) where TOther : class, new() => throw NotSupported("WhereNotExists");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> SelectRaw(string sql) => throw NotSupported("SelectRaw");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> FromRaw(string sql) => throw NotSupported("FromRaw");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> JoinRaw(string sql) => throw NotSupported("JoinRaw");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WithCte(string cteName, string cteQuery) => throw NotSupported("WithCte");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ISqlQueryBuilder<TResult> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery) => throw NotSupported("WithRecursiveCte");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public IWindowedQueryBuilder<TResult> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null) => throw NotSupported("WithWindowFunction");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public ICaseExpressionBuilder<TResult> SelectCase() => throw NotSupported("SelectCase");

        /// <summary>Not supported on projections.</summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public int Delete() => throw NotSupported("Delete");

        /// <summary>Not supported on projections.</summary>
        /// <param name="token">Cancellation token.</param>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public Task<int> DeleteAsync(CancellationToken token = default) => throw NotSupported("Delete");

        IQueryBuilder<TResult> IQueryBuilder<TResult>.Where(Expression<Func<TResult, bool>> predicate) => Where(predicate);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector) => OrderBy(keySelector);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector) => OrderByDescending(keySelector);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.ThenBy<TKey>(Expression<Func<TResult, TKey>> keySelector) => ThenBy(keySelector);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.ThenByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector) => ThenByDescending(keySelector);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.Skip(int count) => Skip(count);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.Take(int count) => Take(count);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.Distinct() => Distinct();

        IQueryBuilder<TResult> IQueryBuilder<TResult>.IgnoreQueryFilters() => IgnoreQueryFilters();

        IQueryBuilder<TResult> IQueryBuilder<TResult>.Include<TProperty>(Expression<Func<TResult, TProperty>> navigationProperty) => Include(navigationProperty);

        IQueryBuilder<TResult> IQueryBuilder<TResult>.ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty) => ThenInclude(navigationProperty);

        IQueryBuilder<TOther> IQueryBuilder<TResult>.Select<TOther>(Expression<Func<TResult, TOther>> selector) => Select(selector);

        #endregion

        #region Private-Methods

        private SqlQueryContext Context => _Source.Context;

        private SelectModel BuildModel(SqlStatementBuilder builder, bool includeOrderingAndPaging)
        {
            SelectModel model = _Source.BuildModel(builder, includeOrderingAndPaging && _Grouping == null);
            TableSource source = new TableSource(SqlQueryBuilder<TSource>.RootAlias, _Source.Metadata);
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            if (_Grouping != null)
            {
                model.GroupBy.AddRange(Context.CreateTranslator(builder).GroupKeySql(_Grouping, source));
                translator.UseGrouping(_Grouping, source);
                foreach (LambdaExpression having in _Grouping.Having) model.Having.Add(translator.Predicate(having.Body));
            }
            else
            {
                translator.Bind(_SelectorParameter, source);
            }

            List<string> selectItems = new List<string>(_Bindings.Count);
            foreach (KeyValuePair<string, Expression> binding in _Bindings)
                selectItems.Add(translator.Value(binding.Value) + " AS " + Context.Dialect.QuoteIdentifier(LabelFor(binding.Key)));
            if (selectItems.Count == 0) throw new NotSupportedException("Select must assign at least one member.");
            model.SelectList = string.Join(", ", selectItems);
            model.Distinct = model.Distinct || _Distinct;

            foreach (Func<SqlExpressionTranslator, string> condition in _Conditions)
            {
                if (_Grouping != null) model.Having.Add(condition(translator));
                else model.Conditions.Add(condition(translator));
            }

            if (includeOrderingAndPaging)
            {
                if (_Orderings.Count > 0)
                {
                    model.OrderBy.Clear();
                    foreach (KeyValuePair<LambdaExpression, bool> ordering in _Orderings)
                        model.OrderBy.Add(translator.Value(Rewrite(ordering.Key)) + Context.Dialect.OrderDirection(ordering.Value));
                }

                if (_Skip.HasValue) model.Skip = _Skip;
                if (_Take.HasValue) model.Take = _Take;
            }

            return model;
        }

        private SqlStatement BuildDerived(string selectList)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = BuildModel(builder, true);
            if (!model.Skip.HasValue && !model.Take.HasValue) model.OrderBy.Clear();
            builder.Append("SELECT ").Append(selectList).Append(" FROM (").Append(model.Render(Context.Dialect)).Append(") dq");
            return builder.Build();
        }

        private Expression Rewrite(LambdaExpression lambda)
        {
            return new MemberReplacer(lambda.Parameters[0], _BindingsByName).Visit(lambda.Body);
        }

        private string LabelFor(string memberName)
        {
            ColumnMetadata? column = _ResultMetadata.FindColumnByProperty(memberName);
            return column?.Name ?? memberName;
        }

        private string Label(LambdaExpression selector)
        {
            Expression body = selector.Body;
            while (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert) body = unary.Operand;
            if (body is MemberExpression member && member.Expression == selector.Parameters[0] && _BindingsByName.ContainsKey(member.Member.Name))
                return Context.Dialect.QuoteIdentifier(LabelFor(member.Member.Name));
            throw new NotSupportedException("Aggregates over a projection require a projected member, for example x => x.Total.");
        }

        private Func<DbDataReader, TResult> CreateMapper()
        {
            return RowMaterializer.CreateMapper<TResult>(_ResultMetadata, Context.Converter);
        }

        private static decimal ToDecimal(object? value)
        {
            return value == null ? 0m : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        private static NotSupportedException NotSupported(string operation)
        {
            return new NotSupportedException(operation + " is not supported on a projected query; apply it before Select.");
        }

        #endregion
    }
}
