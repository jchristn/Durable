namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Processes Include expressions and builds navigation property metadata for SQLite queries.
    /// Handles validation, caching, and relationship discovery for complex entity graphs.
    /// </summary>
    internal class IncludeProcessor
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<Type, string> _TableNameCache = new Dictionary<Type, string>();
        private readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _ColumnMappingCache = new Dictionary<Type, Dictionary<string, PropertyInfo>>();
        private readonly ISanitizer _Sanitizer;
        private readonly IncludeValidator _IncludeValidator;
        private int _AliasCounter = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the IncludeProcessor class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers</param>
        /// <param name="maxIncludeDepth">Maximum depth for nested includes to prevent infinite recursion. Default is 5</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public IncludeProcessor(ISanitizer sanitizer, int maxIncludeDepth = 5)
        {
            _Sanitizer = sanitizer;
            _IncludeValidator = new IncludeValidator(maxIncludeDepth);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Parses a list of include paths and creates corresponding IncludeInfo objects.
        /// </summary>
        /// <typeparam name="T">The root entity type</typeparam>
        /// <param name="includePaths">The navigation property paths to include (e.g., "Company", "Company.Address")</param>
        /// <returns>A list of root-level include information objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when includePaths is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when include validation fails</exception>
        public List<IncludeInfo> ParseIncludes<T>(List<string> includePaths) where T : class
        {
            List<IncludeInfo> rootIncludes = new List<IncludeInfo>();
            Dictionary<string, IncludeInfo> includeMap = new Dictionary<string, IncludeInfo>();

            foreach (string path in includePaths)
            {
                string[] parts = path.Split('.');
                Type currentType = typeof(T);
                IncludeInfo parentInclude = null;
                string currentPath = "";

                for (int i = 0; i < parts.Length; i++)
                {
                    if (i > 0) currentPath += ".";
                    currentPath += parts[i];

                    if (!includeMap.ContainsKey(currentPath))
                    {
                        IncludeInfo includeInfo = CreateIncludeInfo(currentType, parts[i], currentPath, parentInclude);
                        includeMap[currentPath] = includeInfo;

                        if (parentInclude == null)
                        {
                            rootIncludes.Add(includeInfo);
                        }
                        else
                        {
                            parentInclude.Children.Add(includeInfo);
                        }

                        currentType = includeInfo.RelatedEntityType;
                        parentInclude = includeInfo;
                    }
                    else
                    {
                        parentInclude = includeMap[currentPath];
                        currentType = parentInclude.RelatedEntityType;
                    }
                }
            }

            return rootIncludes;
        }

        /// <summary>
        /// Gets the column mappings for the specified entity type, with caching for performance.
        /// </summary>
        /// <param name="entityType">The entity type to get column mappings for</param>
        /// <returns>A dictionary mapping column names to PropertyInfo objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null</exception>
        public Dictionary<string, PropertyInfo> GetColumnMappings(Type entityType)
        {
            if (_ColumnMappingCache.ContainsKey(entityType))
            {
                return _ColumnMappingCache[entityType];
            }

            Dictionary<string, PropertyInfo> columnMappings = new Dictionary<string, PropertyInfo>();

            foreach (PropertyInfo prop in entityType.GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null)
                {
                    columnMappings[attr.Name] = prop;
                }
            }

            _ColumnMappingCache[entityType] = columnMappings;
            return columnMappings;
        }

        #endregion

        #region Private-Methods

        private IncludeInfo CreateIncludeInfo(Type entityType, string propertyName, string propertyPath, IncludeInfo parent)
        {
            PropertyInfo navProp = entityType.GetProperty(propertyName);
            if (navProp == null)
            {
                throw new InvalidOperationException($"Navigation property '{propertyName}' not found on type '{entityType.Name}'");
            }

            Type relatedType = navProp.PropertyType;
            bool isCollection = false;
            PropertyInfo fkProp = null;

            if (relatedType.IsGenericType)
            {
                Type genericTypeDef = relatedType.GetGenericTypeDefinition();
                if (genericTypeDef == typeof(ICollection<>) || genericTypeDef == typeof(List<>) || genericTypeDef == typeof(IList<>))
                {
                    isCollection = true;
                    relatedType = relatedType.GetGenericArguments()[0];
                }
            }

            if (isCollection)
            {
                // Check for many-to-many navigation first
                ManyToManyNavigationPropertyAttribute m2mNavAttr = navProp.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
                if (m2mNavAttr != null)
                {
                    // Handle many-to-many relationships
                    fkProp = entityType.GetProperties().FirstOrDefault(p => p.GetCustomAttribute<PropertyAttribute>()?.Name == "id");
                    if (fkProp == null)
                    {
                        throw new InvalidOperationException($"Primary key property not found on type '{entityType.Name}'");
                    }

                    return new IncludeInfo
                    {
                        PropertyPath = propertyPath,
                        NavigationProperty = navProp,
                        ForeignKeyProperty = fkProp,
                        RelatedEntityType = relatedType,
                        RelatedTableName = GetTableName(relatedType),
                        JoinAlias = $"t{++_AliasCounter}",
                        Parent = parent,
                        IsCollection = isCollection,
                        IsManyToMany = true,
                        JunctionEntityType = m2mNavAttr.JunctionEntityType,
                        JunctionTableName = GetTableName(m2mNavAttr.JunctionEntityType),
                        JunctionAlias = $"j{_AliasCounter}"
                    };
                }
                else
                {
                    // Handle inverse navigation properties (collections)
                    InverseNavigationPropertyAttribute invNavAttr = navProp.GetCustomAttribute<InverseNavigationPropertyAttribute>();
                    if (invNavAttr == null)
                    {
                        throw new InvalidOperationException($"Collection property '{propertyName}' on type '{entityType.Name}' must be marked with InverseNavigationPropertyAttribute or ManyToManyNavigationPropertyAttribute");
                    }

                    // For inverse navigation, the foreign key is on the related entity
                    fkProp = relatedType.GetProperty(invNavAttr.InverseForeignKeyProperty);
                    if (fkProp == null)
                    {
                        throw new InvalidOperationException($"Inverse foreign key property '{invNavAttr.InverseForeignKeyProperty}' not found on type '{relatedType.Name}'");
                    }

                    ForeignKeyAttribute fkAttr = fkProp.GetCustomAttribute<ForeignKeyAttribute>();
                    if (fkAttr == null)
                    {
                        throw new InvalidOperationException($"Inverse foreign key property '{invNavAttr.InverseForeignKeyProperty}' is not marked with ForeignKeyAttribute");
                    }
                }
            }
            else
            {
                // Handle regular navigation properties (single entities)
                NavigationPropertyAttribute navAttr = navProp.GetCustomAttribute<NavigationPropertyAttribute>();
                if (navAttr == null)
                {
                    throw new InvalidOperationException($"Property '{propertyName}' on type '{entityType.Name}' must be marked with NavigationPropertyAttribute");
                }

                fkProp = entityType.GetProperty(navAttr.ForeignKeyProperty);
                if (fkProp == null)
                {
                    throw new InvalidOperationException($"Foreign key property '{navAttr.ForeignKeyProperty}' not found on type '{entityType.Name}'");
                }

                ForeignKeyAttribute fkAttr = fkProp.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr == null)
                {
                    throw new InvalidOperationException($"Foreign key property '{navAttr.ForeignKeyProperty}' is not marked with ForeignKeyAttribute");
                }
            }

            // Validate the include path for cycles and depth
            _IncludeValidator.ValidateInclude(propertyPath, entityType, relatedType);

            return new IncludeInfo
            {
                PropertyPath = propertyPath,
                NavigationProperty = navProp,
                ForeignKeyProperty = fkProp,
                RelatedEntityType = relatedType,
                RelatedTableName = GetTableName(relatedType),
                JoinAlias = $"t{++_AliasCounter}",
                Parent = parent,
                IsCollection = isCollection
            };
        }

        private string GetTableName(Type entityType)
        {
            if (_TableNameCache.ContainsKey(entityType))
            {
                return _TableNameCache[entityType];
            }

            EntityAttribute entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
            {
                throw new InvalidOperationException($"Type {entityType.Name} must have an Entity attribute");
            }

            _TableNameCache[entityType] = entityAttr.Name;
            return entityAttr.Name;
        }

        #endregion
    }
}