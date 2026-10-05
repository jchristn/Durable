namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// The SQL query builder shared by all SQL providers. State is translated into SQL only when the query is built or executed,
    /// so captured variables are read at execution time.
    /// Thread safety: not thread-safe; build and execute on one flow.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class SqlQueryBuilder<T> : ISqlQueryBuilder<T> where T : class, new()
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the execution context. Never null.
        /// </summary>
        public SqlQueryContext Context { get; }

        /// <inheritdoc />
        public string Query => BuildSql();

        #endregion

        #region Private-Members

        internal const string RootAlias = "t0";

        private readonly ITransaction? _Transaction;
        private readonly IReadOnlyList<Expression<Func<T, bool>>> _QueryFilters;
        private readonly List<Func<SqlExpressionTranslator, TableSource, string>> _Conditions = new List<Func<SqlExpressionTranslator, TableSource, string>>();
        private readonly List<OrderClause> _Orderings = new List<OrderClause>();
        private readonly IncludeTree _Includes;
        private readonly List<Func<SqlExpressionTranslator, TableSource, string>> _ExtraSelect = new List<Func<SqlExpressionTranslator, TableSource, string>>();
        private readonly List<string> _Joins = new List<string>();
        private readonly List<string> _Ctes = new List<string>();
        private readonly List<KeyValuePair<SetOperationType, SqlQueryBuilder<T>>> _SetOperations = new List<KeyValuePair<SetOperationType, SqlQueryBuilder<T>>>();
        private int? _Skip;
        private int? _Take;
        private bool _Distinct;
        private bool _IgnoreQueryFilters;
        private bool _RecursiveCte;
        private string? _SelectRaw;
        private string? _FromRaw;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a query builder.
        /// </summary>
        /// <param name="context">Execution context. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="queryFilters">Repository query filters; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
        public SqlQueryBuilder(SqlQueryContext context, ITransaction? transaction = null, IReadOnlyList<Expression<Func<T, bool>>>? queryFilters = null)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Metadata = EntityMetadata.For(typeof(T));
            _Includes = new IncludeTree(Metadata);
            _Transaction = transaction;
            _QueryFilters = queryFilters ?? Array.Empty<Expression<Func<T, bool>>>();
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Where(Expression<Func<T, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            _Conditions.Add((translator, source) => translator.TranslatePredicate(predicate, source));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new OrderClause(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Clear();
            _Orderings.Add(new OrderClause(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new OrderClause(keySelector, false));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            _Orderings.Add(new OrderClause(keySelector, true));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Skip(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Skip count cannot be negative.");
            _Skip = count;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Take(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "Take count cannot be negative.");
            _Take = count;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Distinct()
        {
            _Distinct = true;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> IgnoreQueryFilters()
        {
            _IgnoreQueryFilters = true;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty)
        {
            ArgumentNullException.ThrowIfNull(navigationProperty);
            _Includes.Include(navigationProperty);
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty)
        {
            ArgumentNullException.ThrowIfNull(navigationProperty);
            _Includes.ThenInclude(typeof(TPreviousProperty), navigationProperty);
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new()
        {
            ArgumentNullException.ThrowIfNull(selector);
            return new SqlProjectionQueryBuilder<T, TResult>(this, selector);
        }

        /// <inheritdoc />
        public IGroupedQueryBuilder<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            return new SqlGroupedQueryBuilder<T, TKey>(this, keySelector);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Union(IQueryBuilder<T> other)
        {
            return AddSetOperation(SetOperationType.Union, other);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> UnionAll(IQueryBuilder<T> other)
        {
            return AddSetOperation(SetOperationType.UnionAll, other);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Intersect(IQueryBuilder<T> other)
        {
            return AddSetOperation(SetOperationType.Intersect, other);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Except(IQueryBuilder<T> other)
        {
            return AddSetOperation(SetOperationType.Except, other);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereIn<TKey, TOther>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey) where TOther : class, new()
        {
            return AddInSubquery(keySelector, subquery, subqueryKey, false);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereNotIn<TKey, TOther>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey) where TOther : class, new()
        {
            return AddInSubquery(keySelector, subquery, subqueryKey, true);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql, params object?[] parameters)
        {
            return AddInRaw(keySelector, subquerySql, parameters, false);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereNotInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql, params object?[] parameters)
        {
            return AddInRaw(keySelector, subquerySql, parameters, true);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<T, TOther, bool>>? correlation = null) where TOther : class, new()
        {
            return AddExists(subquery, correlation, false);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<T, TOther, bool>>? correlation = null) where TOther : class, new()
        {
            return AddExists(subquery, correlation, true);
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WhereRaw(string sql, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            object?[] values = parameters ?? Array.Empty<object?>();
            _Conditions.Add((translator, source) => "(" + RawSql.BindPlaceholders(sql, values, translator.Builder, translator.Converter) + ")");
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> SelectRaw(string sql)
        {
            _SelectRaw = sql ?? throw new ArgumentNullException(nameof(sql));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> FromRaw(string sql)
        {
            _FromRaw = sql ?? throw new ArgumentNullException(nameof(sql));
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> JoinRaw(string sql)
        {
            ArgumentNullException.ThrowIfNull(sql);
            _Joins.Add(sql);
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WithCte(string cteName, string cteQuery)
        {
            ArgumentNullException.ThrowIfNull(cteName);
            ArgumentNullException.ThrowIfNull(cteQuery);
            _Ctes.Add(cteName + " AS (" + cteQuery + ")");
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery)
        {
            ArgumentNullException.ThrowIfNull(cteName);
            ArgumentNullException.ThrowIfNull(anchorQuery);
            ArgumentNullException.ThrowIfNull(recursiveQuery);
            _Ctes.Add(cteName + " AS (" + anchorQuery + " UNION ALL " + recursiveQuery + ")");
            _RecursiveCte = true;
            return this;
        }

        /// <inheritdoc />
        public IWindowedQueryBuilder<T> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null)
        {
            return new SqlWindowedQueryBuilder<T>(this, functionName, partitionBy, orderBy);
        }

        /// <inheritdoc />
        public ICaseExpressionBuilder<T> SelectCase()
        {
            return new SqlCaseExpressionBuilder<T>(this);
        }

        /// <inheritdoc />
        public IEnumerable<T> Execute()
        {
            SqlStatement statement = BuildStatement();
            using ConnectionLease lease = Context.Executor.Lease(_Transaction);
            List<T> results = Context.Executor.Query(lease, statement, "SELECT", CreateMapper()).ToList();
            if (!_Includes.IsEmpty) new IncludeLoader(Context.Executor, Context.Converter).Load(lease, results, _Includes.Nodes);
            return results;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> ExecuteAsync(CancellationToken token = default)
        {
            SqlStatement statement = BuildStatement();
            ConnectionLease lease = await Context.Executor.LeaseAsync(_Transaction, token).ConfigureAwait(false);
            await using (lease.ConfigureAwait(false))
            {
                List<T> results = new List<T>();
                await foreach (T item in Context.Executor.QueryAsync(lease, statement, "SELECT", CreateMapper(), token).ConfigureAwait(false))
                    results.Add(item);
                if (!_Includes.IsEmpty)
                    await new IncludeLoader(Context.Executor, Context.Converter).LoadAsync(lease, results, _Includes.Nodes, token).ConfigureAwait(false);
                return results;
            }
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<T> ExecuteAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            SqlStatement statement = BuildStatement();
            if (_Includes.IsEmpty)
            {
                await foreach (T item in Context.Executor.QueryAsync(statement, _Transaction, "SELECT", CreateMapper(), token).ConfigureAwait(false))
                    yield return item;
                yield break;
            }

            if (Context.Executor.ResolveTransaction(_Transaction) != null)
            {
                foreach (T item in await ExecuteAsync(token).ConfigureAwait(false)) yield return item;
                yield break;
            }

            IncludeLoader loader = new IncludeLoader(Context.Executor, Context.Converter);
            int batchSize = Context.Options.IncludeStreamingBatchSize;
            List<T> batch = new List<T>(batchSize);
            ConnectionLease? includeLease = null;
            try
            {
                await foreach (T item in Context.Executor.QueryAsync(statement, null, "SELECT", CreateMapper(), token).ConfigureAwait(false))
                {
                    batch.Add(item);
                    if (batch.Count < batchSize) continue;
                    includeLease ??= await Context.Executor.LeaseAsync(null, token).ConfigureAwait(false);
                    await loader.LoadAsync(includeLease, batch, _Includes.Nodes, token).ConfigureAwait(false);
                    foreach (T loaded in batch) yield return loaded;
                    batch.Clear();
                }

                if (batch.Count > 0)
                {
                    includeLease ??= await Context.Executor.LeaseAsync(null, token).ConfigureAwait(false);
                    await loader.LoadAsync(includeLease, batch, _Includes.Nodes, token).ConfigureAwait(false);
                    foreach (T loaded in batch) yield return loaded;
                }
            }
            finally
            {
                if (includeLease != null) await includeLease.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public IDurableResult<T> ExecuteWithQuery()
        {
            string sql = BuildSql();
            return new DurableResult<T>(sql, Execute());
        }

        /// <inheritdoc />
        public async Task<IDurableResult<T>> ExecuteWithQueryAsync(CancellationToken token = default)
        {
            string sql = BuildSql();
            IEnumerable<T> results = await ExecuteAsync(token).ConfigureAwait(false);
            return new DurableResult<T>(sql, results);
        }

        /// <inheritdoc />
        public IAsyncDurableResult<T> ExecuteAsyncEnumerableWithQuery(CancellationToken token = default)
        {
            return new AsyncDurableResult<T>(BuildSql(), ExecuteAsyncEnumerable(token));
        }

        /// <inheritdoc />
        public long Count()
        {
            SqlStatement statement = BuildCountStatement();
            return Convert.ToInt64(Context.Executor.ExecuteScalar(statement, _Transaction, "COUNT") ?? 0L, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public async Task<long> CountAsync(CancellationToken token = default)
        {
            SqlStatement statement = BuildCountStatement();
            object? value = await Context.Executor.ExecuteScalarAsync(statement, _Transaction, "COUNT", token).ConfigureAwait(false);
            return Convert.ToInt64(value ?? 0L, CultureInfo.InvariantCulture);
        }

        /// <inheritdoc />
        public bool Any()
        {
            SqlStatement statement = BuildAnyStatement();
            return Convert.ToInt64(Context.Executor.ExecuteScalar(statement, _Transaction, "EXISTS") ?? 0L, CultureInfo.InvariantCulture) != 0;
        }

        /// <inheritdoc />
        public async Task<bool> AnyAsync(CancellationToken token = default)
        {
            SqlStatement statement = BuildAnyStatement();
            object? value = await Context.Executor.ExecuteScalarAsync(statement, _Transaction, "EXISTS", token).ConfigureAwait(false);
            return Convert.ToInt64(value ?? 0L, CultureInfo.InvariantCulture) != 0;
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return ToDecimal(Context.Executor.ExecuteScalar(BuildAggregateStatement("SUM", selector), _Transaction, "SUM"));
        }

        /// <inheritdoc />
        public async Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ToDecimal(await Context.Executor.ExecuteScalarAsync(BuildAggregateStatement("SUM", selector), _Transaction, "SUM", token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            return ToDecimal(Context.Executor.ExecuteScalar(BuildAggregateStatement("AVG", selector), _Transaction, "AVG"));
        }

        /// <inheritdoc />
        public async Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            return ToDecimal(await Context.Executor.ExecuteScalarAsync(BuildAggregateStatement("AVG", selector), _Transaction, "AVG", token).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public TProperty Min<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            object? value = Context.Executor.ExecuteScalar(BuildAggregateStatement("MIN", selector), _Transaction, "MIN");
            return ConvertScalar<TProperty>(value, selector);
        }

        /// <inheritdoc />
        public async Task<TProperty> MinAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildAggregateStatement("MIN", selector), _Transaction, "MIN", token).ConfigureAwait(false);
            return ConvertScalar<TProperty>(value, selector);
        }

        /// <inheritdoc />
        public TProperty Max<TProperty>(Expression<Func<T, TProperty>> selector)
        {
            object? value = Context.Executor.ExecuteScalar(BuildAggregateStatement("MAX", selector), _Transaction, "MAX");
            return ConvertScalar<TProperty>(value, selector);
        }

        /// <inheritdoc />
        public async Task<TProperty> MaxAsync<TProperty>(Expression<Func<T, TProperty>> selector, CancellationToken token = default)
        {
            object? value = await Context.Executor.ExecuteScalarAsync(BuildAggregateStatement("MAX", selector), _Transaction, "MAX", token).ConfigureAwait(false);
            return ConvertScalar<TProperty>(value, selector);
        }

        /// <inheritdoc />
        public int Delete()
        {
            return Context.Executor.ExecuteNonQuery(BuildDeleteStatement(), _Transaction, "DELETE");
        }

        /// <inheritdoc />
        public Task<int> DeleteAsync(CancellationToken token = default)
        {
            return Context.Executor.ExecuteNonQueryAsync(BuildDeleteStatement(), _Transaction, "DELETE", token);
        }

        /// <inheritdoc />
        public SqlStatement BuildStatement()
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = BuildModel(builder, true);
            builder.Append(model.Render(Context.Dialect));
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


        IQueryBuilder<T> IQueryBuilder<T>.Where(Expression<Func<T, bool>> predicate) => Where(predicate);

        IQueryBuilder<T> IQueryBuilder<T>.OrderBy<TKey>(Expression<Func<T, TKey>> keySelector) => OrderBy(keySelector);

        IQueryBuilder<T> IQueryBuilder<T>.OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector) => OrderByDescending(keySelector);

        IQueryBuilder<T> IQueryBuilder<T>.ThenBy<TKey>(Expression<Func<T, TKey>> keySelector) => ThenBy(keySelector);

        IQueryBuilder<T> IQueryBuilder<T>.ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector) => ThenByDescending(keySelector);

        IQueryBuilder<T> IQueryBuilder<T>.Skip(int count) => Skip(count);

        IQueryBuilder<T> IQueryBuilder<T>.Take(int count) => Take(count);

        IQueryBuilder<T> IQueryBuilder<T>.Distinct() => Distinct();

        IQueryBuilder<T> IQueryBuilder<T>.IgnoreQueryFilters() => IgnoreQueryFilters();

        IQueryBuilder<T> IQueryBuilder<T>.Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty) => Include(navigationProperty);

        IQueryBuilder<T> IQueryBuilder<T>.ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty) => ThenInclude(navigationProperty);

        IQueryBuilder<TResult> IQueryBuilder<T>.Select<TResult>(Expression<Func<T, TResult>> selector) => Select(selector);

        #endregion

        #region Private-Methods

        internal ITransaction? Transaction => _Transaction;

        internal bool HasPagingOrShaping => _Skip.HasValue || _Take.HasValue || _Distinct || _SetOperations.Count > 0 || _SelectRaw != null;

        internal void AddSelectItem(Func<SqlExpressionTranslator, TableSource, string> item)
        {
            _ExtraSelect.Add(item);
        }

        internal SqlQueryBuilder<T> WhereKey(object?[] keyValues)
        {
            _Conditions.Add((translator, source) => KeyCondition(translator, source, Metadata, keyValues));
            return this;
        }

        internal static string KeyCondition(SqlExpressionTranslator translator, TableSource source, EntityMetadata metadata, object?[] keyValues)
        {
            List<string> parts = new List<string>(metadata.KeyColumns.Count);
            for (int i = 0; i < metadata.KeyColumns.Count; i++)
            {
                ColumnMetadata column = metadata.KeyColumns[i];
                object? value = keyValues[i];
                parts.Add(value == null
                    ? translator.ColumnSql(source, column) + " IS NULL"
                    : translator.ColumnSql(source, column) + " = " + translator.Parameter(value, column));
            }

            return "(" + string.Join(" AND ", parts) + ")";
        }

        internal SelectModel BuildModel(SqlStatementBuilder builder, bool includeOrderingAndPaging)
        {
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            TableSource source = new TableSource(RootAlias, Metadata);
            SelectModel model = new SelectModel();
            ApplyCore(model, translator, source);

            foreach (KeyValuePair<SetOperationType, SqlQueryBuilder<T>> operation in _SetOperations)
            {
                SelectModel other = new SelectModel();
                operation.Value.ApplyCore(other, Context.CreateTranslator(builder), new TableSource(RootAlias, Metadata));
                model.SetOperations.Add(new KeyValuePair<string, string>(SetOperationKeyword(operation.Key), other.RenderCore()));
            }

            if (includeOrderingAndPaging)
            {
                foreach (OrderClause order in _Orderings)
                {
                    SqlExpressionTranslator orderTranslator = Context.CreateTranslator(builder);
                    orderTranslator.Bind(order.KeySelector.Parameters[0], source);
                    model.OrderBy.Add(orderTranslator.Value(order.KeySelector.Body) + Context.Dialect.OrderDirection(order.Descending));
                }

                model.Skip = _Skip;
                model.Take = _Take;
            }

            return model;
        }

        internal void ApplyCore(SelectModel model, SqlExpressionTranslator translator, TableSource source)
        {
            ISqlDialect dialect = Context.Dialect;
            if (_Ctes.Count > 0)
            {
                model.Ctes.AddRange(_Ctes);
                model.RecursiveCte = _RecursiveCte;
            }

            model.Distinct = _Distinct;
            model.From = _FromRaw ?? (dialect.QuoteIdentifier(Metadata.TableName) + " " + RootAlias);
            model.Joins.AddRange(_Joins);

            List<string> selectItems = new List<string> { _SelectRaw ?? (RootAlias + ".*") };
            foreach (Func<SqlExpressionTranslator, TableSource, string> item in _ExtraSelect) selectItems.Add(item(translator, source));
            model.SelectList = string.Join(", ", selectItems);

            model.Conditions.AddRange(RenderConditions(translator, source));
        }

        internal List<string> RenderConditions(SqlExpressionTranslator translator, TableSource source)
        {
            List<string> conditions = new List<string>();
            if (!_IgnoreQueryFilters)
            {
                string? softDelete = translator.SoftDeleteFilter(source);
                if (softDelete != null) conditions.Add(softDelete);
                foreach (Expression<Func<T, bool>> filter in _QueryFilters) conditions.Add(translator.TranslatePredicate(filter, source));
            }

            foreach (Func<SqlExpressionTranslator, TableSource, string> condition in _Conditions) conditions.Add(condition(translator, source));
            return conditions;
        }

        internal SqlStatement BuildDeleteStatement()
        {
            if (HasPagingOrShaping || _FromRaw != null || _Joins.Count > 0)
                throw new NotSupportedException("Delete cannot be combined with Skip, Take, Distinct, set operations, or raw FROM/JOIN/SELECT fragments.");
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            TableSource source = new TableSource(Context.Dialect.QuoteIdentifier(Metadata.TableName), Metadata);
            List<string> conditions = RenderConditions(translator, source);
            SqlWriteBuilder.AppendDelete(builder, Metadata, Context.Converter, conditions);
            return builder.Build();
        }

        internal Func<DbDataReader, T> CreateMapper()
        {
            return RowMaterializer.CreateMapper<T>(Metadata, Context.Converter);
        }

        internal SqlStatement WrapAsDerived(string selectList, Func<SqlExpressionTranslator, TableSource, string>? innerSelect)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = BuildModel(builder, HasPagingOrShaping);
            if (innerSelect != null && _SelectRaw == null && _SetOperations.Count == 0)
            {
                SqlExpressionTranslator translator = Context.CreateTranslator(builder);
                model.SelectList = innerSelect(translator, new TableSource(RootAlias, Metadata));
            }

            if (!model.Skip.HasValue && !model.Take.HasValue) model.OrderBy.Clear();
            builder.Append("SELECT ").Append(selectList).Append(" FROM (").Append(model.Render(Context.Dialect)).Append(") dq");
            return builder.Build();
        }

        private ISqlQueryBuilder<T> AddSetOperation(SetOperationType type, IQueryBuilder<T> other)
        {
            ArgumentNullException.ThrowIfNull(other);
            if (other is not SqlQueryBuilder<T> sqlOther)
                throw new ArgumentException("Set operations require a query from a SQL repository.", nameof(other));
            if (ReferenceEquals(sqlOther, this)) throw new ArgumentException("A query cannot be combined with itself.", nameof(other));
            _SetOperations.Add(new KeyValuePair<SetOperationType, SqlQueryBuilder<T>>(type, sqlOther));
            return this;
        }

        private static string SetOperationKeyword(SetOperationType type)
        {
            switch (type)
            {
                case SetOperationType.Union: return "UNION";
                case SetOperationType.UnionAll: return "UNION ALL";
                case SetOperationType.Intersect: return "INTERSECT";
                case SetOperationType.Except: return "EXCEPT";
                default: throw new NotSupportedException("Unknown set operation " + type + ".");
            }
        }

        private ISqlQueryBuilder<T> AddInSubquery<TKey, TOther>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey, bool negate) where TOther : class, new()
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(subquery);
            ArgumentNullException.ThrowIfNull(subqueryKey);
            if (subquery is not SqlQueryBuilder<TOther> inner)
                throw new ArgumentException("Subqueries require a query from a SQL repository.", nameof(subquery));

            _Conditions.Add((translator, source) =>
            {
                translator.Bind(keySelector.Parameters[0], source);
                string keySql = translator.Value(keySelector.Body);
                string innerAlias = translator.Builder.NextAlias("q");
                SelectModel model = new SelectModel();
                SqlExpressionTranslator innerTranslator = new SqlExpressionTranslator(translator.Builder, translator.Converter, translator.Normalizer.DefaultStringMatching);
                TableSource innerSource = new TableSource(innerAlias, inner.Metadata);
                inner.ApplyCore(model, innerTranslator, innerSource);
                model.From = translator.Dialect.QuoteIdentifier(inner.Metadata.TableName) + " " + innerAlias;
                innerTranslator.Bind(subqueryKey.Parameters[0], innerSource);
                model.SelectList = innerTranslator.Value(subqueryKey.Body);
                return "(" + keySql + (negate ? " NOT IN (" : " IN (") + model.RenderCore() + "))";
            });
            return this;
        }

        private ISqlQueryBuilder<T> AddInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql, object?[] parameters, bool negate)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(subquerySql);
            object?[] values = parameters ?? Array.Empty<object?>();
            _Conditions.Add((translator, source) =>
            {
                translator.Bind(keySelector.Parameters[0], source);
                string keySql = translator.Value(keySelector.Body);
                return "(" + keySql + (negate ? " NOT IN (" : " IN (") + RawSql.BindPlaceholders(subquerySql, values, translator.Builder, translator.Converter) + "))";
            });
            return this;
        }

        private ISqlQueryBuilder<T> AddExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<T, TOther, bool>>? correlation, bool negate) where TOther : class, new()
        {
            ArgumentNullException.ThrowIfNull(subquery);
            if (subquery is not SqlQueryBuilder<TOther> inner)
                throw new ArgumentException("Subqueries require a query from a SQL repository.", nameof(subquery));

            _Conditions.Add((translator, source) =>
            {
                string innerAlias = translator.Builder.NextAlias("q");
                SelectModel model = new SelectModel();
                SqlExpressionTranslator innerTranslator = new SqlExpressionTranslator(translator.Builder, translator.Converter, translator.Normalizer.DefaultStringMatching);
                TableSource innerSource = new TableSource(innerAlias, inner.Metadata);
                inner.ApplyCore(model, innerTranslator, innerSource);
                model.From = translator.Dialect.QuoteIdentifier(inner.Metadata.TableName) + " " + innerAlias;
                model.SelectList = "1";
                if (correlation != null)
                {
                    SqlExpressionTranslator correlationTranslator = new SqlExpressionTranslator(translator.Builder, translator.Converter, translator.Normalizer.DefaultStringMatching);
                    correlationTranslator.Bind(correlation.Parameters[0], source);
                    correlationTranslator.Bind(correlation.Parameters[1], innerSource);
                    model.Conditions.Add(correlationTranslator.Predicate(correlation.Body));
                }

                return (negate ? "NOT EXISTS (" : "EXISTS (") + model.RenderCore() + ")";
            });
            return this;
        }

        private SqlStatement BuildCountStatement()
        {
            if (HasPagingOrShaping) return WrapAsDerived("COUNT(*)", null);
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = BuildModel(builder, false);
            model.SelectList = "COUNT(*)";
            builder.Append(model.Render(Context.Dialect));
            return builder.Build();
        }

        private SqlStatement BuildAnyStatement()
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = BuildModel(builder, HasPagingOrShaping);
            if (!model.Skip.HasValue && !model.Take.HasValue) model.OrderBy.Clear();
            if (!HasPagingOrShaping) model.SelectList = "1";
            builder.Append("SELECT CASE WHEN EXISTS (").Append(model.Render(Context.Dialect)).Append(") THEN 1 ELSE 0 END");
            return builder.Build();
        }

        private SqlStatement BuildAggregateStatement<TProperty>(string function, Expression<Func<T, TProperty>> selector)
        {
            ArgumentNullException.ThrowIfNull(selector);
            string Wrap(string value) => function == "AVG" ? "AVG(CAST(" + value + " AS DECIMAL(38, 10)))" : function + "(" + value + ")";

            if (HasPagingOrShaping)
            {
                return WrapAsDerived(Wrap("dq.agg_value"), (translator, source) =>
                {
                    translator.Bind(selector.Parameters[0], source);
                    return translator.Value(selector.Body) + " AS agg_value";
                });
            }

            SqlStatementBuilder builder = new SqlStatementBuilder(Context.Dialect);
            SelectModel model = BuildModel(builder, false);
            SqlExpressionTranslator translator = Context.CreateTranslator(builder);
            translator.Bind(selector.Parameters[0], new TableSource(RootAlias, Metadata));
            model.SelectList = Wrap(translator.Value(selector.Body));
            builder.Append(model.Render(Context.Dialect));
            return builder.Build();
        }

        private static decimal ToDecimal(object? value)
        {
            if (value == null) return 0m;
            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        private TProperty ConvertScalar<TProperty>(object? value, LambdaExpression selector)
        {
            if (value == null) return default!;
            ColumnMetadata? column = null;
            Expression body = selector.Body;
            while (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert) body = unary.Operand;
            if (body is MemberExpression member && member.Expression is ParameterExpression && member.Member is System.Reflection.PropertyInfo property)
                column = Metadata.FindColumn(property);
            if (column != null && column.PropertyType != typeof(TProperty) && Nullable.GetUnderlyingType(typeof(TProperty)) != column.PropertyType)
                column = null;
            return (TProperty)Context.Converter.ConvertFromDatabase(value, typeof(TProperty), column)!;
        }

        #endregion
    }
}
