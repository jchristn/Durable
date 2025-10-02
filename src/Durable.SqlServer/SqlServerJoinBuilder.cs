namespace Durable.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Builds SQL JOIN clauses and manages column mappings for SQL Server Include operations.
    /// Handles complex navigation property relationships including one-to-many and many-to-many scenarios.
    /// </summary>
    internal class SqlServerJoinBuilder
    {
        #region Public-Members

        /// <summary>
        /// Represents the result of building JOIN SQL with associated metadata.
        /// </summary>
        public class SqlServerJoinResult
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
            public List<SqlServerIncludeInfo> Includes { get; set; } = new List<SqlServerIncludeInfo>();

            /// <summary>
            /// Gets or sets the column mappings organized by table alias for efficient lookup during result mapping.
            /// </summary>
            public Dictionary<string, List<SqlServerColumnMapping>> ColumnMappingsByAlias { get; set; } = new Dictionary<string, List<SqlServerColumnMapping>>();
        }

        /// <summary>
        /// Represents a mapping between a database column and an entity property.
        /// </summary>
        public class SqlServerColumnMapping
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
        private readonly SqlServerIncludeProcessor _IncludeProcessor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlServerJoinBuilder class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers and values</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public SqlServerJoinBuilder(ISanitizer sanitizer)
        {
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _IncludeProcessor = new SqlServerIncludeProcessor(sanitizer);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds SQL JOIN clauses and column mappings for the specified include paths.
        /// </summary>
        /// <typeparam name="T">The base entity type being queried</typeparam>
        /// <param name="baseTableName">The name of the base table</param>
        /// <param name="includePaths">The navigation property paths to include</param>
        /// <returns>A SqlServerJoinResult containing all necessary SQL and mapping information</returns>
        /// <exception cref="ArgumentNullException">Thrown when baseTableName is null</exception>
        public SqlServerJoinResult BuildJoinSql<T>(string baseTableName, List<string> includePaths) where T : class
        {
            if (string.IsNullOrWhiteSpace(baseTableName))
                throw new ArgumentNullException(nameof(baseTableName));

            SqlServerJoinResult result = new SqlServerJoinResult
            {
                ColumnMappingsByAlias = new Dictionary<string, List<SqlServerColumnMapping>>()
            };

            if (includePaths == null || includePaths.Count == 0)
            {
                result.SelectClause = "t0.*";
                result.JoinClause = "";
                result.Includes = new List<SqlServerIncludeInfo>();
                return result;
            }

            List<SqlServerIncludeInfo> includes = _IncludeProcessor.ParseIncludes<T>(includePaths);
            result.Includes = includes;

            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder joinBuilder = new StringBuilder();

            string baseAlias = "t0";
            selectBuilder.Append($"{baseAlias}.*");

            // Add base table column mappings
            Dictionary<string, PropertyInfo> baseColumns = _IncludeProcessor.GetColumnMappings(typeof(T));
            List<SqlServerColumnMapping> baseMappings = new List<SqlServerColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in baseColumns)
            {
                baseMappings.Add(new SqlServerColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = null,
                    Property = kvp.Value,
                    TableAlias = baseAlias
                });
            }
            result.ColumnMappingsByAlias[baseAlias] = baseMappings;

            BuildJoinForIncludes(includes, baseAlias, baseTableName, selectBuilder, joinBuilder, result.ColumnMappingsByAlias, typeof(T));

            result.SelectClause = selectBuilder.ToString();
            result.JoinClause = joinBuilder.ToString();

            return result;
        }

        #endregion

        #region Private-Methods

        private void BuildJoinForIncludes(
            List<SqlServerIncludeInfo> includes,
            string baseAlias,
            string baseTableName,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<SqlServerColumnMapping>> columnMappingsByAlias,
            Type baseEntityType)
        {
            foreach (SqlServerIncludeInfo include in includes)
            {
                // Collections can be handled with LEFT JOINs, though they may create duplicate rows
                // The entity mapper will need to handle grouping the results

                // Build JOIN clause
                string parentAlias = include.Parent?.JoinAlias ?? baseAlias;
                string parentTable = include.Parent?.RelatedTableName ?? baseTableName;

                if (include.IsManyToMany)
                {
                    // Many-to-many requires junction table join
                    BuildManyToManyJoin(include, parentAlias, parentTable, selectBuilder, joinBuilder, columnMappingsByAlias, baseEntityType);
                }
                else
                {
                    // Standard one-to-one or one-to-many join
                    BuildStandardJoin(include, parentAlias, parentTable, selectBuilder, joinBuilder, columnMappingsByAlias, baseEntityType);
                }

                // Recursively build joins for nested includes
                if (include.Children.Count > 0)
                {
                    BuildJoinForIncludes(include.Children, baseAlias, baseTableName, selectBuilder, joinBuilder, columnMappingsByAlias, baseEntityType);
                }
            }
        }

        private void BuildStandardJoin(
            SqlServerIncludeInfo include,
            string parentAlias,
            string parentTable,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<SqlServerColumnMapping>> columnMappingsByAlias,
            Type baseEntityType)
        {
            string sanitizedRelatedTable = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string sanitizedJoinAlias = _Sanitizer.SanitizeIdentifier(include.JoinAlias);

            // Add columns to SELECT clause with explicit aliases
            Dictionary<string, PropertyInfo> relatedColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);

            // Build column mappings for the related table
            List<SqlServerColumnMapping> relatedMappings = new List<SqlServerColumnMapping>();
            List<string> selectColumns = new List<string>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
            {
                string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                string sanitizedColumnName = _Sanitizer.SanitizeIdentifier(kvp.Key);
                selectColumns.Add($"{sanitizedJoinAlias}.{sanitizedColumnName} AS {columnAlias}");

                relatedMappings.Add(new SqlServerColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = columnAlias,
                    Property = kvp.Value,
                    TableAlias = include.JoinAlias
                });
            }

            selectBuilder.Append($", {string.Join(", ", selectColumns)}");
            columnMappingsByAlias[include.JoinAlias] = relatedMappings;

            // Build JOIN clause
            if (include.ForeignKeyProperty != null)
            {
                string foreignKeyColumn = GetColumnNameForProperty(include.ForeignKeyProperty);
                string sanitizedForeignKeyColumn = _Sanitizer.SanitizeIdentifier(foreignKeyColumn);

                joinBuilder.AppendLine();

                if (!string.IsNullOrEmpty(include.InverseForeignKeyProperty))
                {
                    // For inverse navigation properties (collections), the foreign key is on the related table
                    // JOIN condition: related_table.foreign_key = parent_table.primary_key
                    Type parentEntityType = include.Parent?.RelatedEntityType ?? baseEntityType;
                    string parentPrimaryKeyColumn = GetPrimaryKeyColumn(parentEntityType);
                    string sanitizedParentPrimaryKeyColumn = _Sanitizer.SanitizeIdentifier(parentPrimaryKeyColumn);

                    joinBuilder.Append($"LEFT JOIN {sanitizedRelatedTable} {sanitizedJoinAlias} ON {sanitizedJoinAlias}.{sanitizedForeignKeyColumn} = {parentAlias}.{sanitizedParentPrimaryKeyColumn}");
                }
                else
                {
                    // For regular navigation properties, the foreign key is on the parent table
                    // JOIN condition: parent_table.foreign_key = related_table.primary_key
                    string primaryKeyColumn = GetPrimaryKeyColumn(include.RelatedEntityType);
                    string sanitizedPrimaryKeyColumn = _Sanitizer.SanitizeIdentifier(primaryKeyColumn);

                    joinBuilder.Append($"LEFT JOIN {sanitizedRelatedTable} {sanitizedJoinAlias} ON {parentAlias}.{sanitizedForeignKeyColumn} = {sanitizedJoinAlias}.{sanitizedPrimaryKeyColumn}");
                }
            }
        }

        private void BuildManyToManyJoin(
            SqlServerIncludeInfo include,
            string parentAlias,
            string parentTable,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<SqlServerColumnMapping>> columnMappingsByAlias,
            Type baseEntityType)
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
            List<SqlServerColumnMapping> relatedMappings = new List<SqlServerColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
            {
                string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                relatedMappings.Add(new SqlServerColumnMapping
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