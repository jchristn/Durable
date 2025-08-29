namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    internal class IncludeInfo
    {
        public string PropertyPath { get; set; }
        public PropertyInfo NavigationProperty { get; set; }
        public PropertyInfo ForeignKeyProperty { get; set; }
        public Type RelatedEntityType { get; set; }
        public string RelatedTableName { get; set; }
        public string JoinAlias { get; set; }
        public IncludeInfo Parent { get; set; }
        public List<IncludeInfo> Children { get; set; } = new List<IncludeInfo>();
        public bool IsCollection { get; set; }
    }

    internal class IncludeProcessor
    {
        private readonly Dictionary<Type, string> _tableNameCache = new Dictionary<Type, string>();
        private readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _columnMappingCache = new Dictionary<Type, Dictionary<string, PropertyInfo>>();
        private readonly ISanitizer _sanitizer;
        private readonly IncludeValidator _includeValidator;
        private int _aliasCounter = 0;

        public IncludeProcessor(ISanitizer sanitizer, int maxIncludeDepth = 5)
        {
            _sanitizer = sanitizer;
            _includeValidator = new IncludeValidator(maxIncludeDepth);
        }

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

        private IncludeInfo CreateIncludeInfo(Type entityType, string propertyName, string propertyPath, IncludeInfo parent)
        {
            PropertyInfo navProp = entityType.GetProperty(propertyName);
            if (navProp == null)
            {
                throw new InvalidOperationException($"Navigation property '{propertyName}' not found on type '{entityType.Name}'");
            }

            NavigationPropertyAttribute navAttr = navProp.GetCustomAttribute<NavigationPropertyAttribute>();
            if (navAttr == null)
            {
                throw new InvalidOperationException($"Property '{propertyName}' on type '{entityType.Name}' is not marked with NavigationPropertyAttribute");
            }

            PropertyInfo fkProp = entityType.GetProperty(navAttr.ForeignKeyProperty);
            if (fkProp == null)
            {
                throw new InvalidOperationException($"Foreign key property '{navAttr.ForeignKeyProperty}' not found on type '{entityType.Name}'");
            }

            ForeignKeyAttribute fkAttr = fkProp.GetCustomAttribute<ForeignKeyAttribute>();
            if (fkAttr == null)
            {
                throw new InvalidOperationException($"Foreign key property '{navAttr.ForeignKeyProperty}' is not marked with ForeignKeyAttribute");
            }

            Type relatedType = navProp.PropertyType;
            bool isCollection = false;

            if (relatedType.IsGenericType)
            {
                Type genericTypeDef = relatedType.GetGenericTypeDefinition();
                if (genericTypeDef == typeof(ICollection<>) || genericTypeDef == typeof(List<>) || genericTypeDef == typeof(IList<>))
                {
                    isCollection = true;
                    relatedType = relatedType.GetGenericArguments()[0];
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
    }
}