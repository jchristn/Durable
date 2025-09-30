namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Builds SQL JOIN clauses and manages column mappings for PostgreSQL Include operations.
    /// Handles complex navigation property relationships including one-to-many and many-to-many scenarios.
    /// </summary>
    internal class PostgresJoinBuilder
    {

        #region Public-Members

        /// <summary>
        /// Represents the result of building JOIN SQL with associated metadata.
        /// </summary>
        public class PostgresJoinResult
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
            public List<PostgresIncludeInfo> Includes { get; set; } = new List<PostgresIncludeInfo>();

            /// <summary>
            /// Gets or sets the column mappings organized by table alias for efficient lookup during result mapping.
            /// </summary>
            public Dictionary<string, List<PostgresColumnMapping>> ColumnMappingsByAlias { get; set; } = new Dictionary<string, List<PostgresColumnMapping>>();
        }

        /// <summary>
        /// Represents a mapping between a database column and an entity property.
        /// </summary>
        public class PostgresColumnMapping
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
        private readonly PostgresIncludeProcessor _IncludeProcessor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresJoinBuilder class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers and values</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public PostgresJoinBuilder(ISanitizer sanitizer)
        {
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _IncludeProcessor = new PostgresIncludeProcessor(sanitizer);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds SQL JOIN clauses and column mappings for the specified include paths.
        /// </summary>
        /// <typeparam name="T">The base entity type being queried</typeparam>
        /// <param name="baseTableName">The name of the base table</param>
        /// <param name="includePaths">The navigation property paths to include</param>
        /// <returns>A PostgresJoinResult containing all necessary SQL and mapping information</returns>
        /// <exception cref="ArgumentNullException">Thrown when baseTableName is null</exception>
        public PostgresJoinResult BuildJoinSql<T>(string baseTableName, List<string> includePaths) where T : class
        {
            if (string.IsNullOrWhiteSpace(baseTableName))
                throw new ArgumentNullException(nameof(baseTableName));

            PostgresJoinResult result = new PostgresJoinResult
            {
                ColumnMappingsByAlias = new Dictionary<string, List<PostgresColumnMapping>>()
            };

            if (includePaths == null || includePaths.Count == 0)
            {
                result.SelectClause = "t0.*";
                result.JoinClause = "";
                result.Includes = new List<PostgresIncludeInfo>();
                return result;
            }

            List<PostgresIncludeInfo> includes = _IncludeProcessor.ParseIncludes<T>(includePaths);
            result.Includes = includes;

            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder joinBuilder = new StringBuilder();

            string baseAlias = "t0";
            selectBuilder.Append($"{baseAlias}.*");

            // Add base table column mappings
            Dictionary<string, PropertyInfo> baseColumns = _IncludeProcessor.GetColumnMappings(typeof(T));
            List<PostgresColumnMapping> baseMappings = new List<PostgresColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in baseColumns)
            {
                baseMappings.Add(new PostgresColumnMapping
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
            List<PostgresIncludeInfo> includes,
            string baseAlias,
            string baseTableName,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<PostgresColumnMapping>> columnMappingsByAlias,
            Type baseEntityType)
        {
            foreach (PostgresIncludeInfo include in includes)
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
                    // Regular one-to-one or one-to-many join
                    BuildRegularJoin(include, parentAlias, parentTable, selectBuilder, joinBuilder, columnMappingsByAlias);
                }

                // Recursively handle nested includes
                if (include.Children.Count > 0)
                {
                    BuildJoinForIncludes(include.Children, baseAlias, baseTableName, selectBuilder, joinBuilder, columnMappingsByAlias, baseEntityType);
                }
            }
        }

        private void BuildRegularJoin(
            PostgresIncludeInfo include,
            string parentAlias,
            string parentTable,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<PostgresColumnMapping>> columnMappingsByAlias)
        {
            string joinTable = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string joinAlias = include.JoinAlias;

            // Determine join condition based on foreign key relationship
            string joinCondition;
            if (include.ForeignKeyColumn != null)
            {
                // Foreign key is in the current table
                string parentColumn = _Sanitizer.SanitizeIdentifier(include.ForeignKeyColumn);
                string relatedPrimaryKey = _Sanitizer.SanitizeIdentifier(include.RelatedPrimaryKeyColumn);
                joinCondition = $"{parentAlias}.{parentColumn} = {joinAlias}.{relatedPrimaryKey}";
            }
            else if (include.InverseForeignKeyColumn != null)
            {
                // Foreign key is in the related table
                string parentPrimaryKey = _Sanitizer.SanitizeIdentifier(include.ParentPrimaryKeyColumn);
                string inverseForeignKey = _Sanitizer.SanitizeIdentifier(include.InverseForeignKeyColumn);
                joinCondition = $"{parentAlias}.{parentPrimaryKey} = {joinAlias}.{inverseForeignKey}";
            }
            else
            {
                throw new InvalidOperationException($"Unable to determine join condition for include '{include.PropertyName}'");
            }

            // Build the JOIN clause (using LEFT JOIN to include nulls)
            joinBuilder.AppendLine($"LEFT JOIN {joinTable} {joinAlias} ON {joinCondition}");

            // Add columns to SELECT clause
            selectBuilder.Append($", {joinAlias}.*");

            // Add column mappings for the joined table
            Dictionary<string, PropertyInfo> includeColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);
            List<PostgresColumnMapping> includeMappings = new List<PostgresColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in includeColumns)
            {
                includeMappings.Add(new PostgresColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = null,
                    Property = kvp.Value,
                    TableAlias = joinAlias
                });
            }
            columnMappingsByAlias[joinAlias] = includeMappings;
        }

        private void BuildManyToManyJoin(
            PostgresIncludeInfo include,
            string parentAlias,
            string parentTable,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<PostgresColumnMapping>> columnMappingsByAlias,
            Type baseEntityType)
        {
            if (include.JunctionTableName == null)
                throw new InvalidOperationException($"Junction table name is required for many-to-many relationship '{include.PropertyName}'");

            string junctionTable = _Sanitizer.SanitizeIdentifier(include.JunctionTableName);
            string junctionAlias = $"jt_{include.JoinAlias}";
            string targetTable = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string targetAlias = include.JoinAlias;

            // First join to junction table
            string parentPrimaryKey = _Sanitizer.SanitizeIdentifier(include.ParentPrimaryKeyColumn);
            string junctionParentKey = _Sanitizer.SanitizeIdentifier(include.JunctionParentKeyColumn);
            string junctionCondition = $"{parentAlias}.{parentPrimaryKey} = {junctionAlias}.{junctionParentKey}";
            joinBuilder.AppendLine($"LEFT JOIN {junctionTable} {junctionAlias} ON {junctionCondition}");

            // Second join to target table
            string junctionTargetKey = _Sanitizer.SanitizeIdentifier(include.JunctionTargetKeyColumn);
            string targetPrimaryKey = _Sanitizer.SanitizeIdentifier(include.RelatedPrimaryKeyColumn);
            string targetCondition = $"{junctionAlias}.{junctionTargetKey} = {targetAlias}.{targetPrimaryKey}";
            joinBuilder.AppendLine($"LEFT JOIN {targetTable} {targetAlias} ON {targetCondition}");

            // Add columns to SELECT clause
            selectBuilder.Append($", {targetAlias}.*");

            // Add column mappings for the target table
            Dictionary<string, PropertyInfo> includeColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);
            List<PostgresColumnMapping> includeMappings = new List<PostgresColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in includeColumns)
            {
                includeMappings.Add(new PostgresColumnMapping
                {
                    ColumnName = kvp.Key,
                    Alias = null,
                    Property = kvp.Value,
                    TableAlias = targetAlias
                });
            }
            columnMappingsByAlias[targetAlias] = includeMappings;
        }

        #endregion
    }
}