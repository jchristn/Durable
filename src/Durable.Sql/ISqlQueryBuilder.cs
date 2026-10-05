namespace Durable.Sql
{
    using System;
    using System.Linq.Expressions;
    using Durable;

    /// <summary>
    /// SQL query builder: the backend-neutral <see cref="IQueryBuilder{T}"/> plus raw SQL fragments, set operations,
    /// subqueries, CTEs, window functions and CASE expressions. Every value is parameterized; raw fragments accept
    /// <c>{0}</c>-style placeholders that are bound as parameters.
    /// Thread safety: not thread-safe; build and execute on one flow.
    /// </summary>
    /// <typeparam name="T">Entity or result type.</typeparam>
    public interface ISqlQueryBuilder<T> : IQueryBuilder<T> where T : class, new()
    {
        #region Fluent-Overrides

        /// <inheritdoc cref="IQueryBuilder{T}.Where"/>
        new ISqlQueryBuilder<T> Where(Expression<Func<T, bool>> predicate);

        /// <inheritdoc cref="IQueryBuilder{T}.OrderBy"/>
        new ISqlQueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <inheritdoc cref="IQueryBuilder{T}.OrderByDescending"/>
        new ISqlQueryBuilder<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <inheritdoc cref="IQueryBuilder{T}.ThenBy"/>
        new ISqlQueryBuilder<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <inheritdoc cref="IQueryBuilder{T}.ThenByDescending"/>
        new ISqlQueryBuilder<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector);

        /// <inheritdoc cref="IQueryBuilder{T}.Skip"/>
        new ISqlQueryBuilder<T> Skip(int count);

        /// <inheritdoc cref="IQueryBuilder{T}.Take"/>
        new ISqlQueryBuilder<T> Take(int count);

        /// <inheritdoc cref="IQueryBuilder{T}.Distinct"/>
        new ISqlQueryBuilder<T> Distinct();

        /// <inheritdoc cref="IQueryBuilder{T}.IgnoreQueryFilters"/>
        new ISqlQueryBuilder<T> IgnoreQueryFilters();

        /// <inheritdoc cref="IQueryBuilder{T}.Include"/>
        new ISqlQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> navigationProperty);

        /// <inheritdoc cref="IQueryBuilder{T}.ThenInclude"/>
        new ISqlQueryBuilder<T> ThenInclude<TPreviousProperty, TProperty>(Expression<Func<TPreviousProperty, TProperty>> navigationProperty);

        /// <inheritdoc cref="IQueryBuilder{T}.Select"/>
        new ISqlQueryBuilder<TResult> Select<TResult>(Expression<Func<T, TResult>> selector) where TResult : class, new();

        #endregion

        #region Set-Operations

        /// <summary>
        /// Combines with another query of the same type, removing duplicates. Ordering and paging of this builder apply to the combined result.
        /// </summary>
        /// <param name="other">Other query from a SQL repository of the same provider. Must not be null.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException">Thrown when other is null.</exception>
        /// <exception cref="ArgumentException">Thrown when other is not a SQL query builder.</exception>
        ISqlQueryBuilder<T> Union(IQueryBuilder<T> other);

        /// <summary>
        /// Combines with another query of the same type, keeping duplicates.
        /// </summary>
        /// <param name="other">Other query. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> UnionAll(IQueryBuilder<T> other);

        /// <summary>
        /// Keeps rows present in both queries.
        /// </summary>
        /// <param name="other">Other query. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> Intersect(IQueryBuilder<T> other);

        /// <summary>
        /// Removes rows present in the other query.
        /// </summary>
        /// <param name="other">Other query. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> Except(IQueryBuilder<T> other);

        #endregion

        #region Subqueries

        /// <summary>
        /// Keeps rows whose key is among the values selected by a subquery.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <typeparam name="TOther">Subquery entity type.</typeparam>
        /// <param name="keySelector">Key on this entity. Must not be null.</param>
        /// <param name="subquery">Subquery. Must not be null.</param>
        /// <param name="subqueryKey">Column selected by the subquery. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WhereIn<TKey, TOther>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey) where TOther : class, new();

        /// <summary>
        /// Keeps rows whose key is not among the values selected by a subquery.
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <typeparam name="TOther">Subquery entity type.</typeparam>
        /// <param name="keySelector">Key on this entity. Must not be null.</param>
        /// <param name="subquery">Subquery. Must not be null.</param>
        /// <param name="subqueryKey">Column selected by the subquery. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WhereNotIn<TKey, TOther>(Expression<Func<T, TKey>> keySelector, IQueryBuilder<TOther> subquery, Expression<Func<TOther, TKey>> subqueryKey) where TOther : class, new();

        /// <summary>
        /// Keeps rows whose key is among the values returned by raw SQL (first column).
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key on this entity. Must not be null.</param>
        /// <param name="subquerySql">Subquery SQL; may contain {0}-style placeholders. Must not be null.</param>
        /// <param name="parameters">Placeholder values.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WhereInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql, params object?[] parameters);

        /// <summary>
        /// Keeps rows whose key is not among the values returned by raw SQL (first column).
        /// </summary>
        /// <typeparam name="TKey">Key type.</typeparam>
        /// <param name="keySelector">Key on this entity. Must not be null.</param>
        /// <param name="subquerySql">Subquery SQL; may contain {0}-style placeholders. Must not be null.</param>
        /// <param name="parameters">Placeholder values.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WhereNotInRaw<TKey>(Expression<Func<T, TKey>> keySelector, string subquerySql, params object?[] parameters);

        /// <summary>
        /// Keeps rows for which the subquery returns at least one row, optionally correlated, for example
        /// <c>WhereExists(books, (author, book) =&gt; book.AuthorId == author.Id)</c>.
        /// </summary>
        /// <typeparam name="TOther">Subquery entity type.</typeparam>
        /// <param name="subquery">Subquery. Must not be null.</param>
        /// <param name="correlation">Correlation between the outer and inner rows; null for an uncorrelated EXISTS.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WhereExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<T, TOther, bool>>? correlation = null) where TOther : class, new();

        /// <summary>
        /// Keeps rows for which the subquery returns no rows, optionally correlated.
        /// </summary>
        /// <typeparam name="TOther">Subquery entity type.</typeparam>
        /// <param name="subquery">Subquery. Must not be null.</param>
        /// <param name="correlation">Correlation between the outer and inner rows; null for an uncorrelated NOT EXISTS.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WhereNotExists<TOther>(IQueryBuilder<TOther> subquery, Expression<Func<T, TOther, bool>>? correlation = null) where TOther : class, new();

        #endregion

        #region Raw-SQL

        /// <summary>
        /// Adds a raw SQL condition. <c>{0}</c>, <c>{1}</c>... placeholders are bound as parameters; columns may be
        /// referenced unqualified or as <c>t0.column</c>.
        /// </summary>
        /// <param name="sql">Condition SQL. Must not be null.</param>
        /// <param name="parameters">Placeholder values.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        ISqlQueryBuilder<T> WhereRaw(string sql, params object?[] parameters);

        /// <summary>
        /// Replaces the select list with raw SQL. Result columns are mapped to <typeparamref name="T"/> by name.
        /// </summary>
        /// <param name="sql">Select list SQL. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> SelectRaw(string sql);

        /// <summary>
        /// Replaces the FROM source with raw SQL (for example, a CTE name or a derived table). Alias it t0 to keep
        /// generated column references valid.
        /// </summary>
        /// <param name="sql">FROM source SQL. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> FromRaw(string sql);

        /// <summary>
        /// Appends a raw JOIN clause.
        /// </summary>
        /// <param name="sql">JOIN clause including the JOIN keyword. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> JoinRaw(string sql);

        /// <summary>
        /// Adds a common table expression.
        /// </summary>
        /// <param name="cteName">CTE name. Must not be null.</param>
        /// <param name="cteQuery">CTE body SQL. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WithCte(string cteName, string cteQuery);

        /// <summary>
        /// Adds a recursive common table expression (anchor UNION ALL recursive member).
        /// </summary>
        /// <param name="cteName">CTE name, optionally with a column list, for example "tree(id, parent_id)". Must not be null.</param>
        /// <param name="anchorQuery">Anchor member SQL. Must not be null.</param>
        /// <param name="recursiveQuery">Recursive member SQL. Must not be null.</param>
        /// <returns>This builder.</returns>
        ISqlQueryBuilder<T> WithRecursiveCte(string cteName, string anchorQuery, string recursiveQuery);

        /// <summary>
        /// Starts a window function added to the select list.
        /// </summary>
        /// <param name="functionName">Function name, for example ROW_NUMBER. Used when no typed function is chosen.</param>
        /// <param name="partitionBy">Raw PARTITION BY list; null for none.</param>
        /// <param name="orderBy">Raw ORDER BY list; null for none.</param>
        /// <returns>A window builder.</returns>
        IWindowedQueryBuilder<T> WithWindowFunction(string functionName, string? partitionBy = null, string? orderBy = null);

        /// <summary>
        /// Starts a CASE expression added to the select list.
        /// </summary>
        /// <returns>A CASE builder.</returns>
        ICaseExpressionBuilder<T> SelectCase();

        #endregion

        #region Introspection

        /// <summary>
        /// Builds the SELECT statement for the current definition with its parameters.
        /// </summary>
        /// <returns>The statement.</returns>
        SqlStatement BuildStatement();

        /// <summary>
        /// Builds the SELECT statement and returns its text (with parameter placeholders).
        /// </summary>
        /// <returns>SQL text.</returns>
        string BuildSql();

        #endregion
    }
}
