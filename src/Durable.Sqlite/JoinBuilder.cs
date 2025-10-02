namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Builds SQL JOIN clauses and manages column mappings for SQLite Include operations.
    /// Handles complex navigation property relationships including one-to-many and many-to-many scenarios.
    /// </summary>
    internal class JoinBuilder
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly ISanitizer _Sanitizer;
        private readonly IncludeProcessor _IncludeProcessor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the JoinBuilder class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public JoinBuilder(ISanitizer sanitizer)
        {
            _Sanitizer = sanitizer;
            _IncludeProcessor = new IncludeProcessor(sanitizer);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Represents the result of building JOIN SQL with associated metadata.
        /// </summary>
        public class JoinResult
        {
            /// <summary>
            /// Gets or sets the SELECT clause with all required columns and aliases.
            /// </summary>
            public string SelectClause { get; set; }

            /// <summary>
            /// Gets or sets the JOIN clause containing all necessary table joins.
            /// </summary>
            public string JoinClause { get; set; }

            /// <summary>
            /// Gets or sets the collection of include information for entity mapping.
            /// </summary>
            public List<IncludeInfo> Includes { get; set; }

            /// <summary>
            /// Gets or sets the column mappings organized by table alias for efficient lookup during result mapping.
            /// </summary>
            public Dictionary<string, List<ColumnMapping>> ColumnMappingsByAlias { get; set; }
        }

        /// <summary>
        /// Represents a mapping between a database column and an entity property.
        /// </summary>
        public class ColumnMapping
        {
            /// <summary>
            /// Gets or sets the database column name.
            /// </summary>
            public string ColumnName { get; set; }

            /// <summary>
            /// Gets or sets the SQL alias for the column in SELECT statements.
            /// </summary>
            public string Alias { get; set; }

            /// <summary>
            /// Gets or sets the property that this column maps to.
            /// </summary>
            public PropertyInfo Property { get; set; }

            /// <summary>
            /// Gets or sets the table alias that owns this column.
            /// </summary>
            public string TableAlias { get; set; }
        }

        /// <summary>
        /// Builds JOIN SQL statements with SELECT and JOIN clauses for the specified includes.
        /// </summary>
        /// <typeparam name="T">The root entity type</typeparam>
        /// <param name="baseTableName">The name of the base table</param>
        /// <param name="includePaths">The navigation property paths to include</param>
        /// <returns>A JoinResult containing the SELECT clause, JOIN clause, and mapping information</returns>
        /// <exception cref="ArgumentNullException">Thrown when baseTableName is null</exception>
        public JoinResult BuildJoinSql<T>(string baseTableName, List<string> includePaths) where T : class
        {
            JoinResult result = new JoinResult
            {
                ColumnMappingsByAlias = new Dictionary<string, List<ColumnMapping>>()
            };

            if (includePaths == null || includePaths.Count == 0)
            {
                result.SelectClause = "t0.*";
                result.JoinClause = "";
                result.Includes = new List<IncludeInfo>();
                return result;
            }

            List<IncludeInfo> includes = _IncludeProcessor.ParseIncludes<T>(includePaths);
            result.Includes = includes;

            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder joinBuilder = new StringBuilder();

            string baseAlias = "t0";
            selectBuilder.Append($"{baseAlias}.*");

            Dictionary<string, PropertyInfo> baseColumns = _IncludeProcessor.GetColumnMappings(typeof(T));
            List<ColumnMapping> baseMappings = new List<ColumnMapping>();
            foreach (KeyValuePair<string, PropertyInfo> kvp in baseColumns)
            {
                baseMappings.Add(new ColumnMapping
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
            List<IncludeInfo> includes,
            string parentAlias,
            string parentTableName,
            StringBuilder selectBuilder,
            StringBuilder joinBuilder,
            Dictionary<string, List<ColumnMapping>> columnMappingsByAlias)
        {
            foreach (IncludeInfo include in includes)
            {
                if (include.IsCollection && !include.IsManyToMany)
                {
                    // Skip one-to-many collections (handled by CollectionLoader)
                    continue;
                }

                if (include.IsManyToMany)
                {
                    // Handle many-to-many relationships with junction table
                    string parentPkColumn = GetPrimaryKeyColumn(include.ForeignKeyProperty.DeclaringType);

                    Dictionary<string, PropertyInfo> relatedColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);
                    List<ColumnMapping> mappings = new List<ColumnMapping>();

                    foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
                    {
                        string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                        string sanitizedColumn = _Sanitizer.SanitizeIdentifier(kvp.Key);
                        selectBuilder.Append($", {include.JoinAlias}.{sanitizedColumn} AS {_Sanitizer.SanitizeIdentifier(columnAlias)}");
                        
                        mappings.Add(new ColumnMapping
                        {
                            ColumnName = kvp.Key,
                            Alias = columnAlias,
                            Property = kvp.Value,
                            TableAlias = include.JoinAlias
                        });
                    }

                    columnMappingsByAlias[include.JoinAlias] = mappings;

                    string relatedPkColumn = GetPrimaryKeyColumn(include.RelatedEntityType);

                    joinBuilder.AppendLine();
                    joinBuilder.Append($"LEFT JOIN {_Sanitizer.SanitizeIdentifier(include.JunctionTableName)} {include.JunctionAlias} ");
                    joinBuilder.Append($"ON {parentAlias}.{_Sanitizer.SanitizeIdentifier(parentPkColumn)} = {include.JunctionAlias}.{_Sanitizer.SanitizeIdentifier(GetJunctionForeignKeyColumn(include, true))}");

                    joinBuilder.AppendLine();
                    joinBuilder.Append($"LEFT JOIN {_Sanitizer.SanitizeIdentifier(include.RelatedTableName)} {include.JoinAlias} ");
                    joinBuilder.Append($"ON {include.JunctionAlias}.{_Sanitizer.SanitizeIdentifier(GetJunctionForeignKeyColumn(include, false))} = {include.JoinAlias}.{_Sanitizer.SanitizeIdentifier(relatedPkColumn)}");
                }
                else
                {
                    // Handle regular one-to-one relationships
                    ForeignKeyAttribute fkAttr = include.ForeignKeyProperty.GetCustomAttribute<ForeignKeyAttribute>();
                    PropertyAttribute fkPropAttr = include.ForeignKeyProperty.GetCustomAttribute<PropertyAttribute>();
                    string fkColumnName = fkPropAttr?.Name ?? include.ForeignKeyProperty.Name;

                    Dictionary<string, PropertyInfo> relatedColumns = _IncludeProcessor.GetColumnMappings(include.RelatedEntityType);
                    List<ColumnMapping> mappings = new List<ColumnMapping>();

                    foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
                    {
                        string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                        string sanitizedColumn = _Sanitizer.SanitizeIdentifier(kvp.Key);
                        selectBuilder.Append($", {include.JoinAlias}.{sanitizedColumn} AS {_Sanitizer.SanitizeIdentifier(columnAlias)}");
                        
                        mappings.Add(new ColumnMapping
                        {
                            ColumnName = kvp.Key,
                            Alias = columnAlias,
                            Property = kvp.Value,
                            TableAlias = include.JoinAlias
                        });
                    }

                    columnMappingsByAlias[include.JoinAlias] = mappings;

                    string referencedColumn = GetPrimaryKeyColumn(include.RelatedEntityType);

                    joinBuilder.AppendLine();
                    joinBuilder.Append($"LEFT JOIN {_Sanitizer.SanitizeIdentifier(include.RelatedTableName)} {include.JoinAlias} ");
                    joinBuilder.Append($"ON {parentAlias}.{_Sanitizer.SanitizeIdentifier(fkColumnName)} = {include.JoinAlias}.{_Sanitizer.SanitizeIdentifier(referencedColumn)}");
                }

                if (include.Children.Count > 0)
                {
                    BuildJoinForIncludes(
                        include.Children,
                        include.JoinAlias,
                        include.RelatedTableName,
                        selectBuilder,
                        joinBuilder,
                        columnMappingsByAlias);
                }
            }
        }

        private string GetPrimaryKeyColumn(Type entityType)
        {
            foreach (PropertyInfo prop in entityType.GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return attr.Name;
                }
            }
            throw new InvalidOperationException($"No primary key found for type {entityType.Name}");
        }

        private string GetJunctionForeignKeyColumn(IncludeInfo include, bool forThisEntity)
        {
            ManyToManyNavigationPropertyAttribute m2mAttr = include.NavigationProperty.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
            if (m2mAttr == null)
            {
                throw new InvalidOperationException($"ManyToManyNavigationPropertyAttribute not found for {include.PropertyPath}");
            }

            if (forThisEntity)
            {
                // Get the foreign key property for the source entity
                PropertyInfo fkProp = include.JunctionEntityType.GetProperties()
                    .FirstOrDefault(p => p.Name == m2mAttr.ThisEntityForeignKeyProperty);
                if (fkProp == null)
                {
                    throw new InvalidOperationException($"Junction foreign key property '{m2mAttr.ThisEntityForeignKeyProperty}' not found");
                }
                PropertyAttribute propAttr = fkProp.GetCustomAttribute<PropertyAttribute>();
                return propAttr?.Name ?? fkProp.Name;
            }
            else
            {
                // Get the foreign key property for the related entity
                PropertyInfo fkProp = include.JunctionEntityType.GetProperties()
                    .FirstOrDefault(p => p.Name == m2mAttr.RelatedEntityForeignKeyProperty);
                if (fkProp == null)
                {
                    throw new InvalidOperationException($"Junction foreign key property '{m2mAttr.RelatedEntityForeignKeyProperty}' not found");
                }
                PropertyAttribute propAttr = fkProp.GetCustomAttribute<PropertyAttribute>();
                return propAttr?.Name ?? fkProp.Name;
            }
        }

        #endregion
    }
}