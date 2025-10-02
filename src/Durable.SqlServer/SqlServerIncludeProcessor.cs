namespace Durable.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Processes Include expressions and builds navigation property metadata for MySQL queries.
    /// Handles validation, caching, and relationship discovery for complex entity graphs.
    /// </summary>
    internal class SqlServerIncludeProcessor
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<Type, string> _TableNameCache = new Dictionary<Type, string>();
        private readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _ColumnMappingCache = new Dictionary<Type, Dictionary<string, PropertyInfo>>();
        private readonly ISanitizer _Sanitizer;
        private readonly SqlServerIncludeValidator _IncludeValidator;
        private int _AliasCounter = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlServerIncludeProcessor class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers</param>
        /// <param name="maxIncludeDepth">Maximum depth for nested includes to prevent infinite recursion. Default is 5</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public SqlServerIncludeProcessor(ISanitizer sanitizer, int maxIncludeDepth = 5)
        {
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _IncludeValidator = new SqlServerIncludeValidator(maxIncludeDepth);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Parses a list of include paths and creates corresponding SqlServerIncludeInfo objects.
        /// </summary>
        /// <typeparam name="T">The root entity type</typeparam>
        /// <param name="includePaths">The navigation property paths to include (e.g., "Company", "Company.Address")</param>
        /// <returns>A list of root-level include information objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when includePaths is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when include validation fails</exception>
        public List<SqlServerIncludeInfo> ParseIncludes<T>(List<string> includePaths) where T : class
        {
            if (includePaths == null)
                throw new ArgumentNullException(nameof(includePaths));

            List<SqlServerIncludeInfo> rootIncludes = new List<SqlServerIncludeInfo>();
            Dictionary<string, SqlServerIncludeInfo> includeMap = new Dictionary<string, SqlServerIncludeInfo>();

            foreach (string path in includePaths)
            {
                if (string.IsNullOrWhiteSpace(path))
                    continue;

                _IncludeValidator.ValidateIncludePath(path);

                string[] parts = path.Split('.');
                Type currentType = typeof(T);
                SqlServerIncludeInfo? parentInclude = null;
                string currentPath = "";

                for (int i = 0; i < parts.Length; i++)
                {
                    if (i > 0) currentPath += ".";
                    currentPath += parts[i];

                    if (!includeMap.ContainsKey(currentPath))
                    {
                        SqlServerIncludeInfo includeInfo = CreateIncludeInfo(currentType, parts[i], currentPath, parentInclude);
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
        /// Gets the column mappings for the specified entity type, using caching for performance.
        /// </summary>
        /// <param name="entityType">The entity type to get column mappings for</param>
        /// <returns>A dictionary mapping column names to PropertyInfo objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null</exception>
        public Dictionary<string, PropertyInfo> GetColumnMappings(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            if (_ColumnMappingCache.TryGetValue(entityType, out Dictionary<string, PropertyInfo>? cached))
            {
                return cached;
            }

            Dictionary<string, PropertyInfo> columnMappings = new Dictionary<string, PropertyInfo>();
            PropertyInfo[] properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (PropertyInfo property in properties)
            {
                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && !string.IsNullOrWhiteSpace(attr.Name))
                {
                    columnMappings[attr.Name] = property;
                }
            }

            _ColumnMappingCache[entityType] = columnMappings;
            return columnMappings;
        }

        /// <summary>
        /// Gets the table name for the specified entity type, using caching for performance.
        /// </summary>
        /// <param name="entityType">The entity type to get the table name for</param>
        /// <returns>The database table name</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type doesn't have an EntityAttribute</exception>
        public string GetTableName(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            if (_TableNameCache.TryGetValue(entityType, out string? cached))
            {
                return cached;
            }

            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null || string.IsNullOrWhiteSpace(entityAttr.Name))
            {
                throw new InvalidOperationException($"Entity type {entityType.Name} must have an EntityAttribute with a valid Name");
            }

            _TableNameCache[entityType] = entityAttr.Name;
            return entityAttr.Name;
        }

        #endregion

        #region Private-Methods

        private SqlServerIncludeInfo CreateIncludeInfo(Type parentType, string propertyName, string fullPath, SqlServerIncludeInfo? parent)
        {
            PropertyInfo? navigationProperty = parentType.GetProperty(propertyName);
            if (navigationProperty == null)
            {
                throw new InvalidOperationException($"Navigation property '{propertyName}' not found on type '{parentType.Name}'");
            }

            // Determine if this is a collection property
            bool isCollection = IsCollectionProperty(navigationProperty);
            Type relatedEntityType = isCollection
                ? GetCollectionElementType(navigationProperty.PropertyType)
                : navigationProperty.PropertyType;

            SqlServerIncludeInfo includeInfo = new SqlServerIncludeInfo
            {
                PropertyPath = fullPath,
                NavigationProperty = navigationProperty,
                RelatedEntityType = relatedEntityType,
                RelatedTableName = GetTableName(relatedEntityType),
                JoinAlias = GenerateAlias(),
                Parent = parent,
                IsCollection = isCollection
            };

            // Look for relationship attributes
            SetupRelationshipInfo(includeInfo, parentType, navigationProperty);

            return includeInfo;
        }

        private void SetupRelationshipInfo(SqlServerIncludeInfo includeInfo, Type parentType, PropertyInfo navigationProperty)
        {
            // Check for ForeignKey attribute
            ForeignKeyAttribute? foreignKeyAttr = navigationProperty.GetCustomAttribute<ForeignKeyAttribute>();
            if (foreignKeyAttr != null)
            {
                PropertyInfo? foreignKeyProperty = parentType.GetProperty(foreignKeyAttr.ReferencedProperty);
                if (foreignKeyProperty != null)
                {
                    includeInfo.ForeignKeyProperty = foreignKeyProperty;
                }
            }

            // Check for ManyToMany attribute
            ManyToManyNavigationPropertyAttribute? manyToManyAttr = navigationProperty.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
            if (manyToManyAttr != null)
            {
                includeInfo.IsManyToMany = true;
                includeInfo.JunctionEntityType = manyToManyAttr.JunctionEntityType;

                // Get junction table name from the junction entity type
                if (manyToManyAttr.JunctionEntityType != null)
                {
                    includeInfo.JunctionTableName = GetTableName(manyToManyAttr.JunctionEntityType);
                    includeInfo.JunctionAlias = GenerateAlias();
                }
            }

            // Check for NavigationProperty attribute
            NavigationPropertyAttribute? navPropAttr = navigationProperty.GetCustomAttribute<NavigationPropertyAttribute>();
            if (navPropAttr != null && !string.IsNullOrEmpty(navPropAttr.ForeignKeyProperty))
            {
                PropertyInfo? foreignKeyProperty = parentType.GetProperty(navPropAttr.ForeignKeyProperty);
                if (foreignKeyProperty != null)
                {
                    includeInfo.ForeignKeyProperty = foreignKeyProperty;
                }
            }

            // Check for InverseNavigationProperty attribute (indicates collection navigation)
            InverseNavigationPropertyAttribute? inverseAttr = navigationProperty.GetCustomAttribute<InverseNavigationPropertyAttribute>();
            if (inverseAttr != null)
            {
                includeInfo.IsCollection = true;

                // For inverse navigation, the foreign key is on the related entity
                Type relatedEntityType = includeInfo.RelatedEntityType;
                PropertyInfo? inverseForeignKeyProperty = relatedEntityType.GetProperty(inverseAttr.InverseForeignKeyProperty);

                if (inverseForeignKeyProperty == null)
                {
                    throw new InvalidOperationException($"Inverse foreign key property '{inverseAttr.InverseForeignKeyProperty}' not found on type '{relatedEntityType.Name}'");
                }

                ForeignKeyAttribute? fkAttr = inverseForeignKeyProperty.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr == null)
                {
                    throw new InvalidOperationException($"Inverse foreign key property '{inverseAttr.InverseForeignKeyProperty}' is not marked with ForeignKeyAttribute");
                }

                includeInfo.ForeignKeyProperty = inverseForeignKeyProperty;
                includeInfo.InverseForeignKeyProperty = inverseAttr.InverseForeignKeyProperty;
            }
        }

        private bool IsCollectionProperty(PropertyInfo property)
        {
            Type propertyType = property.PropertyType;

            // Check if it's a generic collection type
            if (propertyType.IsGenericType)
            {
                Type genericTypeDefinition = propertyType.GetGenericTypeDefinition();
                return genericTypeDefinition == typeof(List<>) ||
                       genericTypeDefinition == typeof(IList<>) ||
                       genericTypeDefinition == typeof(ICollection<>) ||
                       genericTypeDefinition == typeof(IEnumerable<>);
            }

            return false;
        }

        private Type GetCollectionElementType(Type collectionType)
        {
            if (collectionType.IsGenericType)
            {
                return collectionType.GetGenericArguments()[0];
            }

            throw new InvalidOperationException($"Cannot determine element type for collection type {collectionType.Name}");
        }

        private string GenerateAlias()
        {
            return $"t{++_AliasCounter}";
        }

        #endregion
    }
}