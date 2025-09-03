namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    internal class IncludeInfo
    {
        #region Public-Members

        public string PropertyPath { get; set; }
        public PropertyInfo NavigationProperty { get; set; }
        public PropertyInfo ForeignKeyProperty { get; set; }
        public Type RelatedEntityType { get; set; }
        public string RelatedTableName { get; set; }
        public string JoinAlias { get; set; }
        public IncludeInfo Parent { get; set; }
        public List<IncludeInfo> Children { get; set; } = new List<IncludeInfo>();
        public bool IsCollection { get; set; }
        public bool IsManyToMany { get; set; }
        public Type JunctionEntityType { get; set; }
        public string JunctionTableName { get; set; }
        public string JunctionAlias { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }

    internal class IncludeProcessor
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<Type, string> _tableNameCache = new Dictionary<Type, string>();
        private readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _columnMappingCache = new Dictionary<Type, Dictionary<string, PropertyInfo>>();
        private readonly ISanitizer _sanitizer;
        private readonly IncludeValidator _includeValidator;
        private int _aliasCounter = 0;

        #endregion

        #region Constructors-and-Factories

        public IncludeProcessor(ISanitizer sanitizer, int maxIncludeDepth = 5)
        {
            _sanitizer = sanitizer;
            _includeValidator = new IncludeValidator(maxIncludeDepth);
        }

        #endregion

        #region Public-Methods

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

        public Dictionary<string, PropertyInfo> GetColumnMappings(Type entityType)
        {
            if (_columnMappingCache.ContainsKey(entityType))
            {
                return _columnMappingCache[entityType];
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

            _columnMappingCache[entityType] = columnMappings;
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
                        JoinAlias = $"t{++_aliasCounter}",
                        Parent = parent,
                        IsCollection = isCollection,
                        IsManyToMany = true,
                        JunctionEntityType = m2mNavAttr.JunctionEntityType,
                        JunctionTableName = GetTableName(m2mNavAttr.JunctionEntityType),
                        JunctionAlias = $"j{_aliasCounter}"
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
            _includeValidator.ValidateInclude(propertyPath, entityType, relatedType);

            return new IncludeInfo
            {
                PropertyPath = propertyPath,
                NavigationProperty = navProp,
                ForeignKeyProperty = fkProp,
                RelatedEntityType = relatedType,
                RelatedTableName = GetTableName(relatedType),
                JoinAlias = $"t{++_aliasCounter}",
                Parent = parent,
                IsCollection = isCollection
            };
        }

        private string GetTableName(Type entityType)
        {
            if (_tableNameCache.ContainsKey(entityType))
            {
                return _tableNameCache[entityType];
            }

            EntityAttribute entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
            {
                throw new InvalidOperationException($"Type {entityType.Name} must have an Entity attribute");
            }

            _tableNameCache[entityType] = entityAttr.Name;
            return entityAttr.Name;
        }

        #endregion
    }
}