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
        /// This is a simplified implementation that creates basic include stubs.
        /// </summary>
        /// <typeparam name="T">The root entity type</typeparam>
        /// <param name="includePaths">The navigation property paths to include</param>
        /// <returns>A list of root-level include information objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when includePaths is null</exception>
        public List<PostgresIncludeInfo> ParseIncludes<T>(List<string> includePaths) where T : class
        {
            if (includePaths == null)
                throw new ArgumentNullException(nameof(includePaths));

            // For now, return empty list to enable compilation
            // Full implementation would parse the navigation property paths
            return new List<PostgresIncludeInfo>();
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
                // Skip navigation properties for now
                if (IsNavigationProperty(property))
                    continue;

                // Get column name from PropertyAttribute or use property name
                string columnName = GetColumnName(property);
                mappings[columnName] = property;
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