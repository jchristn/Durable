namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Builds SQL JOIN clauses and manages column mappings for MySQL Include operations.
    /// Handles complex navigation property relationships including one-to-many and many-to-many scenarios.
    /// </summary>
    internal class MySqlJoinBuilder
    {
        #region Public-Members

        /// <summary>
        /// Represents the result of building JOIN SQL with associated metadata.
        /// </summary>
        public class MySqlJoinResult
        {
            /// <summary>
            /// Gets or sets the SELECT clause with all required columns and aliases.
            /// </summary>
            public string SelectClause { get; set; } = string.Empty;

            /// <summary>
            /// Gets or sets the JOIN clause containing all necessary table joins.
            /// </summary>
            public string JoinClause { get; set; } = string.Empty;

            /// <summary>
            /// Gets or sets the collection of include information for entity mapping.
            /// </summary>
            public List<MySqlIncludeInfo> Includes { get; set; } = new List<MySqlIncludeInfo>();

            /// <summary>
            /// Gets or sets the column mappings organized by table alias for efficient lookup during result mapping.
            /// </summary>
            public Dictionary<string, List<MySqlColumnMapping>> ColumnMappingsByAlias { get; set; } = new Dictionary<string, List<MySqlColumnMapping>>();
        }

        /// <summary>
        /// Represents a mapping between a database column and an entity property.
        /// </summary>
        public class MySqlColumnMapping
        {
            /// <summary>
            /// Gets or sets the database column name.
            /// </summary>
            public string ColumnName { get; set; } = string.Empty;

            /// <summary>
            /// Gets or sets the SQL alias for the column in SELECT statements.
            /// </summary>
            public string? Alias { get; set; }

            /// <summary>
            /// Gets or sets the property that this column maps to.
            /// </summary>
            public PropertyInfo Property { get; set; } = null!;

            /// <summary>
            /// Gets or sets the table alias that owns this column.
            /// </summary>
            public string TableAlias { get; set; } = string.Empty;
        }

        #endregion

        #region Private-Members

        private readonly ISanitizer _Sanitizer;
        private readonly MySqlIncludeProcessor _IncludeProcessor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlJoinBuilder class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers and values</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public MySqlJoinBuilder(ISanitizer sanitizer)
        {
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _IncludeProcessor = new MySqlIncludeProcessor(sanitizer);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds SQL JOIN clauses and column mappings for the specified include paths.
        /// </summary>
        /// <typeparam name="T">The base entity type being queried</typeparam>
        /// <param name="baseTableName">The name of the base table</param>
        /// <param name="includePaths">The navigation property paths to include</param>
        /// <returns>A MySqlJoinResult containing all necessary SQL and mapping information</returns>
        /// <exception cref="ArgumentNullException">Thrown when baseTableName is null</exception>
        public MySqlJoinResult BuildJoinSql<T>(string baseTableName, List<string> includePaths) where T : class
        {
            if (string.IsNullOrWhiteSpace(baseTableName))
                throw new ArgumentNullException(nameof(baseTableName));

            MySqlJoinResult result = new MySqlJoinResult
            {
                ColumnMappingsByAlias = new Dictionary<string, List<MySqlColumnMapping>>()
            };

            if (includePaths == null || includePaths.Count == 0)
            {
                result.SelectClause = "t0.*";
                result.JoinClause = "";
                result.Includes = new List<MySqlIncludeInfo>();
                return result;
            }

            List<MySqlIncludeInfo> includes = _IncludeProcessor.ParseIncludes<T>(includePaths);
            result.Includes = includes;

            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder joinBuilder = new StringBuilder();

            string baseAlias = "t0";
            selectBuilder.Append($"{baseAlias}.*");

            // Add base table column mappings
            Dictionary<string, PropertyInfo> baseColumns = _IncludeProcessor.GetColumnMappings(typeof(T));
            List<MySqlColumnMapping> baseMappings = new List<MySqlColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in baseColumns)
            {
                baseMappings.Add(new MySqlColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = null,
                    Property = kvp.Value,
                    TableAlias = baseAlias
                });
            }
            result.ColumnMappingsByAlias[baseAlias] = baseMappings;

            BuildJoinForIncludes(includes, baseAlias, baseTableName, selectBuilder, joinBuilder, result.ColumnMappingsByAlias);

            result.SelectClause = selectBuilder.ToString();
            result.JoinClause = joinBuilder.ToString();

            return result;
        }

        #endregion

        #region Private-Methods

        private void BuildJoinForIncludes(
            List<MySqlIncludeInfo> includes,
            string baseAlias,
            string baseTableName,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<MySqlColumnMapping>> columnMappingsByAlias)
        {
            foreach (MySqlIncludeInfo include in includes)
            {
                if (include.IsCollection)
                {
                    // Collections require special handling and are not supported in this basic implementation
                    continue;
                }

                // Build JOIN clause
                string parentAlias = include.Parent?.JoinAlias ?? baseAlias;
                string parentTable = include.Parent?.RelatedTableName ?? baseTableName;

                if (include.IsManyToMany)
                {
                    // Many-to-many requires junction table join
                    BuildManyToManyJoin(include, parentAlias, parentTable, selectBuilder, joinBuilder, columnMappingsByAlias);
                }
                else
                {
                    // Standard one-to-one or one-to-many join
                    BuildStandardJoin(include, parentAlias, parentTable, selectBuilder, joinBuilder, columnMappingsByAlias);
                }

                // Recursively build joins for nested includes
                if (include.Children.Count > 0)
                {
                    BuildJoinForIncludes(include.Children, baseAlias, baseTableName, selectBuilder, joinBuilder, columnMappingsByAlias);
                }
            }
        }

        private void BuildStandardJoin(
            MySqlIncludeInfo include,
            string parentAlias,
            string parentTable,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<MySqlColumnMapping>> columnMappingsByAlias)
        {
            string sanitizedRelatedTable = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string sanitizedJoinAlias = _Sanitizer.SanitizeIdentifier(include.JoinAlias);

            // Add columns to SELECT clause
            Dictionary<string, PropertyInfo> relatedColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);
            selectBuilder.Append($", {sanitizedJoinAlias}.*");

            // Build column mappings for the related table
            List<MySqlColumnMapping> relatedMappings = new List<MySqlColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
            {
                string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                relatedMappings.Add(new MySqlColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = columnAlias,
                    Property = kvp.Value,
                    TableAlias = include.JoinAlias
                });
            }
            columnMappingsByAlias[include.JoinAlias] = relatedMappings;

            // Build JOIN clause
            if (include.ForeignKeyProperty != null)
            {
                string foreignKeyColumn = GetColumnNameForProperty(include.ForeignKeyProperty);
                string sanitizedForeignKeyColumn = _Sanitizer.SanitizeIdentifier(foreignKeyColumn);

                // Determine the primary key column of the related table
                string primaryKeyColumn = GetPrimaryKeyColumn(include.RelatedEntityType);
                string sanitizedPrimaryKeyColumn = _Sanitizer.SanitizeIdentifier(primaryKeyColumn);

                joinBuilder.AppendLine();
                joinBuilder.Append($"LEFT JOIN {sanitizedRelatedTable} {sanitizedJoinAlias} ON {parentAlias}.{sanitizedForeignKeyColumn} = {sanitizedJoinAlias}.{sanitizedPrimaryKeyColumn}");
            }
        }

        private void BuildManyToManyJoin(
            MySqlIncludeInfo include,
            string parentAlias,
            string parentTable,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<MySqlColumnMapping>> columnMappingsByAlias)
        {
            if (string.IsNullOrEmpty(include.JunctionTableName) || string.IsNullOrEmpty(include.JunctionAlias))
            {
                throw new InvalidOperationException($"Many-to-many relationship for {include.PropertyPath} requires junction table information");
            }

            string sanitizedJunctionTable = _Sanitizer.SanitizeIdentifier(include.JunctionTableName);
            string sanitizedJunctionAlias = _Sanitizer.SanitizeIdentifier(include.JunctionAlias);
            string sanitizedRelatedTable = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string sanitizedJoinAlias = _Sanitizer.SanitizeIdentifier(include.JoinAlias);

            // Add columns to SELECT clause
            Dictionary<string, PropertyInfo> relatedColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);
            selectBuilder.Append($", {sanitizedJoinAlias}.*");

            // Build column mappings for the related table
            List<MySqlColumnMapping> relatedMappings = new List<MySqlColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
            {
                string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                relatedMappings.Add(new MySqlColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = columnAlias,
                    Property = kvp.Value,
                    TableAlias = include.JoinAlias
                });
            }
            columnMappingsByAlias[include.JoinAlias] = relatedMappings;

            // Build the two-step JOIN for many-to-many
            // First join to junction table, then to target table
            string parentPrimaryKey = GetPrimaryKeyColumn(typeof(object)); // This would need proper parent type resolution
            string relatedPrimaryKey = GetPrimaryKeyColumn(include.RelatedEntityType);

            joinBuilder.AppendLine();
            joinBuilder.Append($"LEFT JOIN {sanitizedJunctionTable} {sanitizedJunctionAlias} ON {parentAlias}.{_Sanitizer.SanitizeIdentifier(parentPrimaryKey)} = {sanitizedJunctionAlias}.{parentAlias}_id");
            joinBuilder.AppendLine();
            joinBuilder.Append($"LEFT JOIN {sanitizedRelatedTable} {sanitizedJoinAlias} ON {sanitizedJunctionAlias}.{include.JoinAlias}_id = {sanitizedJoinAlias}.{_Sanitizer.SanitizeIdentifier(relatedPrimaryKey)}");
        }

        private string GetColumnNameForProperty(PropertyInfo property)
        {
            PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
            return attr?.Name ?? property.Name;
        }

        private string GetPrimaryKeyColumn(Type entityType)
        {
            Dictionary<string, PropertyInfo> columns = _IncludeProcessor.GetColumnMappings(entityType);
            foreach (KeyValuePair<string, PropertyInfo> kvp in columns)
            {
                PropertyAttribute? attr = kvp.Value.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return kvp.Key;
                }
            }
            return "id"; // Default fallback
        }

        #endregion
    }
}