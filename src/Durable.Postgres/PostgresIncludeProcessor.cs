namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Simplified PostgreSQL include processor for basic navigation property support.
    /// This is a minimal implementation to enable compilation and basic functionality.
    /// </summary>
    internal class PostgresIncludeProcessor
    {

        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<Type, string> _TableNameCache = new Dictionary<Type, string>();
        private readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _ColumnMappingCache = new Dictionary<Type, Dictionary<string, PropertyInfo>>();
        private readonly ISanitizer _Sanitizer;
        private readonly PostgresIncludeValidator _IncludeValidator;
        private int _AliasCounter = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresIncludeProcessor class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer to use for SQL identifiers</param>
        /// <param name="maxIncludeDepth">Maximum depth for nested includes to prevent infinite recursion. Default is 5</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer is null</exception>
        public PostgresIncludeProcessor(ISanitizer sanitizer, int maxIncludeDepth = 5)
        {
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _IncludeValidator = new PostgresIncludeValidator(maxIncludeDepth);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Parses a list of include paths and creates corresponding PostgresIncludeInfo objects.
        /// Supports nested navigation properties through dot notation (e.g., "Company.Address").
        /// </summary>
        /// <typeparam name="T">The root entity type</typeparam>
        /// <param name="includePaths">The navigation property paths to include</param>
        /// <returns>A list of root-level include information objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when includePaths is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when include validation fails</exception>
        public List<PostgresIncludeInfo> ParseIncludes<T>(List<string> includePaths) where T : class
        {
            if (includePaths == null)
                throw new ArgumentNullException(nameof(includePaths));

            List<PostgresIncludeInfo> rootIncludes = new List<PostgresIncludeInfo>();
            Dictionary<string, PostgresIncludeInfo> includeMap = new Dictionary<string, PostgresIncludeInfo>();

            foreach (string path in includePaths)
            {
                if (string.IsNullOrWhiteSpace(path))
                    continue;

                _IncludeValidator.ValidateIncludePath(path);

                string[] parts = path.Split('.');
                Type currentType = typeof(T);
                PostgresIncludeInfo? parentInclude = null;
                string currentPath = "";

                for (int i = 0; i < parts.Length; i++)
                {
                    if (i > 0) currentPath += ".";
                    currentPath += parts[i];

                    if (!includeMap.ContainsKey(currentPath))
                    {
                        PostgresIncludeInfo includeInfo = CreateIncludeInfo(currentType, parts[i], currentPath, parentInclude);
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
        /// Gets the column mappings for the specified entity type.
        /// This method caches results to improve performance for repeated calls.
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

            Dictionary<string, PropertyInfo> mappings = new Dictionary<string, PropertyInfo>();

            PropertyInfo[] properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo property in properties)
            {
                // Only include properties with PropertyAttribute
                PropertyAttribute? propAttr = property.GetCustomAttribute<PropertyAttribute>();
                if (propAttr != null)
                {
                    mappings[propAttr.Name] = property;
                }
            }

            _ColumnMappingCache[entityType] = mappings;
            return mappings;
        }

        /// <summary>
        /// Gets the database table name for the specified entity type.
        /// This method caches results to improve performance.
        /// </summary>
        /// <param name="entityType">The entity type to get the table name for</param>
        /// <returns>The database table name</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type does not have an EntityAttribute</exception>
        public string GetTableName(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            if (_TableNameCache.TryGetValue(entityType, out string? cached))
            {
                return cached;
            }

            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Entity type '{entityType.Name}' must have an EntityAttribute");

            _TableNameCache[entityType] = entityAttr.Name;
            return entityAttr.Name;
        }

        #endregion

        #region Private-Methods

        private PostgresIncludeInfo CreateIncludeInfo(Type parentType, string propertyName, string fullPath, PostgresIncludeInfo? parent)
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

            PostgresIncludeInfo includeInfo = new PostgresIncludeInfo
            {
                PropertyName = propertyName,
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

        private void SetupRelationshipInfo(PostgresIncludeInfo includeInfo, Type parentType, PropertyInfo navigationProperty)
        {
            // Check for NavigationProperty attribute (standard many-to-one)
            NavigationPropertyAttribute? navAttr = navigationProperty.GetCustomAttribute<NavigationPropertyAttribute>();
            if (navAttr != null)
            {
                PropertyInfo? foreignKeyProperty = parentType.GetProperty(navAttr.ForeignKeyProperty);
                if (foreignKeyProperty != null)
                {
                    includeInfo.ForeignKeyProperty = foreignKeyProperty;
                    includeInfo.ForeignKeyColumn = GetColumnName(foreignKeyProperty);
                }
            }

            // Check for ForeignKey attribute (alternative syntax)
            ForeignKeyAttribute? foreignKeyAttr = navigationProperty.GetCustomAttribute<ForeignKeyAttribute>();
            if (foreignKeyAttr != null)
            {
                PropertyInfo? foreignKeyProperty = parentType.GetProperty(foreignKeyAttr.ReferencedProperty);
                if (foreignKeyProperty != null)
                {
                    includeInfo.ForeignKeyProperty = foreignKeyProperty;
                    includeInfo.ForeignKeyColumn = GetColumnName(foreignKeyProperty);
                }
            }

            // Check for ManyToMany attribute
            ManyToManyNavigationPropertyAttribute? manyToManyAttr = navigationProperty.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
            if (manyToManyAttr != null)
            {
                includeInfo.IsManyToMany = true;
                includeInfo.JunctionEntityType = manyToManyAttr.JunctionEntityType;

                if (includeInfo.JunctionEntityType != null)
                {
                    includeInfo.JunctionTableName = GetTableName(includeInfo.JunctionEntityType);
                    includeInfo.JunctionAlias = GenerateAlias();
                    includeInfo.JunctionParentKeyColumn = manyToManyAttr.ThisEntityForeignKeyProperty;
                    includeInfo.JunctionTargetKeyColumn = manyToManyAttr.RelatedEntityForeignKeyProperty;
                }
            }

            // Check for InverseNavigationProperty attribute
            InverseNavigationPropertyAttribute? inverseAttr = navigationProperty.GetCustomAttribute<InverseNavigationPropertyAttribute>();
            if (inverseAttr != null)
            {
                includeInfo.InverseForeignKeyProperty = inverseAttr.InverseForeignKeyProperty;

                // For inverse properties, the foreign key is on the related entity pointing back to us
                PropertyInfo? relatedProperty = includeInfo.RelatedEntityType.GetProperty(inverseAttr.InverseForeignKeyProperty);
                if (relatedProperty != null)
                {
                    includeInfo.InverseForeignKeyColumn = GetColumnName(relatedProperty);
                }
            }

            // Set primary key columns
            includeInfo.ParentPrimaryKeyColumn = GetPrimaryKeyColumn(parentType);
            includeInfo.RelatedPrimaryKeyColumn = GetPrimaryKeyColumn(includeInfo.RelatedEntityType);
        }

        private bool IsCollectionProperty(PropertyInfo property)
        {
            Type propertyType = property.PropertyType;

            // Check if it's a generic collection type (ICollection<T>, IList<T>, List<T>, etc.)
            if (propertyType.IsGenericType)
            {
                Type genericTypeDefinition = propertyType.GetGenericTypeDefinition();
                return genericTypeDefinition == typeof(ICollection<>) ||
                       genericTypeDefinition == typeof(IList<>) ||
                       genericTypeDefinition == typeof(List<>) ||
                       genericTypeDefinition == typeof(IEnumerable<>);
            }

            // Check if it implements IEnumerable but is not a string
            return typeof(System.Collections.IEnumerable).IsAssignableFrom(propertyType) &&
                   propertyType != typeof(string);
        }

        private Type GetCollectionElementType(Type collectionType)
        {
            if (collectionType.IsGenericType)
            {
                Type[] genericArgs = collectionType.GetGenericArguments();
                if (genericArgs.Length > 0)
                {
                    return genericArgs[0];
                }
            }

            throw new InvalidOperationException($"Cannot determine element type for collection type '{collectionType.Name}'");
        }

        private string GenerateAlias()
        {
            return $"t{++_AliasCounter}";
        }

        private string GetPrimaryKeyColumn(Type entityType)
        {
            PropertyInfo[] properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (PropertyInfo property in properties)
            {
                PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return GetColumnName(property);
                }
            }

            // Default to "id" if no primary key is found
            return "id";
        }

        private string GetColumnName(PropertyInfo property)
        {
            PropertyAttribute? propAttr = property.GetCustomAttribute<PropertyAttribute>();
            return propAttr?.Name ?? property.Name.ToLowerInvariant();
        }

        private bool IsNavigationProperty(PropertyInfo property)
        {
            // Check if it has navigation property attributes
            if (property.GetCustomAttribute<NavigationPropertyAttribute>() != null ||
                property.GetCustomAttribute<InverseNavigationPropertyAttribute>() != null ||
                property.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>() != null)
            {
                return true;
            }

            // Check if it's a complex type or collection that's not a basic type
            Type propertyType = property.PropertyType;
            if (propertyType.IsClass && propertyType != typeof(string) && !IsBasicType(propertyType))
            {
                return true;
            }

            // Check if it's a collection of complex types
            if (typeof(System.Collections.IEnumerable).IsAssignableFrom(propertyType) && propertyType != typeof(string))
            {
                Type? elementType = propertyType.GetGenericArguments().FirstOrDefault();
                if (elementType != null && elementType.IsClass && !IsBasicType(elementType))
                {
                    return true;
                }
            }

            return false;
        }

        private bool IsBasicType(Type type)
        {
            return type.IsPrimitive ||
                   type == typeof(string) ||
                   type == typeof(DateTime) ||
                   type == typeof(DateTimeOffset) ||
                   type == typeof(TimeSpan) ||
                   type == typeof(Guid) ||
                   type == typeof(decimal) ||
                   Nullable.GetUnderlyingType(type) != null;
        }

        #endregion
    }
}