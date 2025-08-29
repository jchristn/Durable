namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    internal class JoinBuilder
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly ISanitizer _sanitizer;
        private readonly IncludeProcessor _includeProcessor;

        #endregion

        #region Constructors-and-Factories

        public JoinBuilder(ISanitizer sanitizer)
        {
            _sanitizer = sanitizer;
            _includeProcessor = new IncludeProcessor(sanitizer);
        }

        #endregion

        #region Public-Methods

        public class JoinResult
        {
            public string SelectClause { get; set; }
            public string JoinClause { get; set; }
            public List<IncludeInfo> Includes { get; set; }
            public Dictionary<string, List<ColumnMapping>> ColumnMappingsByAlias { get; set; }
        }

        public class ColumnMapping
        {
            public string ColumnName { get; set; }
            public string Alias { get; set; }
            public PropertyInfo Property { get; set; }
            public string TableAlias { get; set; }
        }

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

            List<IncludeInfo> includes = _includeProcessor.ParseIncludes<T>(includePaths);
            result.Includes = includes;

            StringBuilder selectBuilder = new StringBuilder();
            StringBuilder joinBuilder = new StringBuilder();

            string baseAlias = "t0";
            selectBuilder.Append($"{baseAlias}.*");

            Dictionary<string, PropertyInfo> baseColumns = _includeProcessor.GetColumnMappings(typeof(T));
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
                if (include.IsCollection)
                {
                    continue;
                }

                ForeignKeyAttribute fkAttr = include.ForeignKeyProperty.GetCustomAttribute<ForeignKeyAttribute>();
                PropertyAttribute fkPropAttr = include.ForeignKeyProperty.GetCustomAttribute<PropertyAttribute>();
                string fkColumnName = fkPropAttr?.Name ?? include.ForeignKeyProperty.Name;

                Dictionary<string, PropertyInfo> relatedColumns = _includeProcessor.GetColumnMappings(include.RelatedEntityType);
                List<ColumnMapping> mappings = new List<ColumnMapping>();

                foreach (KeyValuePair<string, PropertyInfo> kvp in relatedColumns)
                {
                    string columnAlias = $"{include.JoinAlias}_{kvp.Key}";
                    selectBuilder.Append($", {include.JoinAlias}.{_sanitizer.SanitizeIdentifier(kvp.Key)} AS {columnAlias}");
                    
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
                joinBuilder.Append($"LEFT JOIN {_sanitizer.SanitizeIdentifier(include.RelatedTableName)} {include.JoinAlias} ");
                joinBuilder.Append($"ON {parentAlias}.{_sanitizer.SanitizeIdentifier(fkColumnName)} = {include.JoinAlias}.{_sanitizer.SanitizeIdentifier(referencedColumn)}");

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

        #endregion
    }
}