namespace Durable.MySql
{
    using System;
    using System.Linq.Expressions;
    using System.Text;

    /// <summary>
    /// Extension methods for MySQL full-text search capabilities.
    /// Provides methods to add MATCH() AGAINST() clauses to queries.
    /// </summary>
    public static class MySqlFullTextSearchExtensions
    {

        #region Public-Members

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Adds a full-text search condition using MATCH() AGAINST() syntax for a single column.
        /// </summary>
        /// <typeparam name="T">The entity type</typeparam>
        /// <typeparam name="TProperty">The property type being searched</typeparam>
        /// <param name="queryBuilder">The query builder instance</param>
        /// <param name="propertySelector">Expression selecting the property to search</param>
        /// <param name="searchTerms">The search terms to match against</param>
        /// <param name="searchMode">The full-text search mode to use. Default is Natural.</param>
        /// <returns>The query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when queryBuilder, propertySelector, or searchTerms is null</exception>
        /// <exception cref="ArgumentException">Thrown when searchTerms is empty or whitespace</exception>
        public static IQueryBuilder<T> WhereFullTextMatch<T, TProperty>(
            this IQueryBuilder<T> queryBuilder,
            Expression<Func<T, TProperty>> propertySelector,
            string searchTerms,
            FullTextSearchMode searchMode = FullTextSearchMode.Natural) where T : class, new()
        {
            if (queryBuilder == null)
                throw new ArgumentNullException(nameof(queryBuilder));

            if (propertySelector == null)
                throw new ArgumentNullException(nameof(propertySelector));

            if (searchTerms == null)
                throw new ArgumentNullException(nameof(searchTerms));

            if (string.IsNullOrWhiteSpace(searchTerms))
                throw new ArgumentException("Search terms cannot be empty or whitespace", nameof(searchTerms));

            string columnName = GetPropertyName(propertySelector);
            string searchModeSql = GetSearchModeSql(searchMode);
            string escapedSearchTerms = EscapeSearchTerms(searchTerms);

            string sql = $"MATCH(`{columnName}`) AGAINST('{escapedSearchTerms}' {searchModeSql})";

            return queryBuilder.WhereRaw(sql);
        }

        /// <summary>
        /// Adds a full-text search condition using MATCH() AGAINST() syntax for multiple columns.
        /// All specified columns must be part of the same FULLTEXT index.
        /// </summary>
        /// <typeparam name="T">The entity type</typeparam>
        /// <param name="queryBuilder">The query builder instance</param>
        /// <param name="propertySelectors">Array of expressions selecting the properties to search</param>
        /// <param name="searchTerms">The search terms to match against</param>
        /// <param name="searchMode">The full-text search mode to use. Default is Natural.</param>
        /// <returns>The query builder for method chaining</returns>
        /// <exception cref="ArgumentNullException">Thrown when queryBuilder, propertySelectors, or searchTerms is null</exception>
        /// <exception cref="ArgumentException">Thrown when propertySelectors is empty or searchTerms is empty/whitespace</exception>
        public static IQueryBuilder<T> WhereFullTextMatch<T>(
            this IQueryBuilder<T> queryBuilder,
            Expression<Func<T, object>>[] propertySelectors,
            string searchTerms,
            FullTextSearchMode searchMode = FullTextSearchMode.Natural) where T : class, new()
        {
            if (queryBuilder == null)
                throw new ArgumentNullException(nameof(queryBuilder));

            if (propertySelectors == null)
                throw new ArgumentNullException(nameof(propertySelectors));

            if (propertySelectors.Length == 0)
                throw new ArgumentException("At least one property selector must be provided", nameof(propertySelectors));

            if (searchTerms == null)
                throw new ArgumentNullException(nameof(searchTerms));

            if (string.IsNullOrWhiteSpace(searchTerms))
                throw new ArgumentException("Search terms cannot be empty or whitespace", nameof(searchTerms));

            StringBuilder columnList = new StringBuilder();
            for (int i = 0; i < propertySelectors.Length; i++)
            {
                if (i > 0)
                    columnList.Append(", ");

                string columnName = GetPropertyName(propertySelectors[i]);
                columnList.Append($"`{columnName}`");
            }

            string searchModeSql = GetSearchModeSql(searchMode);
            string escapedSearchTerms = EscapeSearchTerms(searchTerms);

            string sql = $"MATCH({columnList}) AGAINST('{escapedSearchTerms}' {searchModeSql})";

            return queryBuilder.WhereRaw(sql);
        }


        #endregion

        #region Private-Methods

        private static string GetPropertyName<T, TProperty>(Expression<Func<T, TProperty>> propertySelector)
        {
            if (propertySelector.Body is MemberExpression memberExpression)
            {
                return memberExpression.Member.Name;
            }
            else if (propertySelector.Body is UnaryExpression unaryExpression &&
                     unaryExpression.Operand is MemberExpression unaryMember)
            {
                return unaryMember.Member.Name;
            }

            throw new ArgumentException("Property selector must be a simple property access expression", nameof(propertySelector));
        }

        private static string GetSearchModeSql(FullTextSearchMode searchMode)
        {
            switch (searchMode)
            {
                case FullTextSearchMode.Natural:
                    return "IN NATURAL LANGUAGE MODE";

                case FullTextSearchMode.NaturalWithQueryExpansion:
                    return "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION";

                case FullTextSearchMode.Boolean:
                    return "IN BOOLEAN MODE";

                case FullTextSearchMode.BooleanWithQueryExpansion:
                    return "WITH QUERY EXPANSION";

                default:
                    throw new ArgumentException($"Unsupported search mode: {searchMode}", nameof(searchMode));
            }
        }

        private static string EscapeSearchTerms(string searchTerms)
        {
            if (string.IsNullOrEmpty(searchTerms))
                return searchTerms;

            // Escape single quotes by doubling them (SQL standard)
            return searchTerms.Replace("'", "''");
        }

        #endregion
    }
}
