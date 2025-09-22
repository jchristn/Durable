namespace Durable.MySql
{
    using MySqlConnector;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Advanced entity mapping capabilities for MySQL data readers.
    /// Handles complex type conversions, navigation properties, and relationship mapping
    /// with support for Include operations and nested entity graphs.
    /// </summary>
    /// <typeparam name="T">The primary entity type being mapped</typeparam>
    internal class MySqlEntityMapper<T> where T : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly IDataTypeConverter _DataTypeConverter;
        private readonly Dictionary<string, PropertyInfo> _BaseColumnMappings;
        private readonly Dictionary<string, HashSet<object>> _ProcessedEntities;
        private readonly ISanitizer _Sanitizer;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlEntityMapper class.
        /// </summary>
        /// <param name="dataTypeConverter">The data type converter for handling complex type conversions</param>
        /// <param name="baseColumnMappings">Column mappings for the primary entity type</param>
        /// <param name="sanitizer">The sanitizer for handling SQL values and identifiers</param>
        /// <exception cref="ArgumentNullException">Thrown when any required parameter is null</exception>
        public MySqlEntityMapper(
            IDataTypeConverter dataTypeConverter,
            Dictionary<string, PropertyInfo> baseColumnMappings,
            ISanitizer sanitizer)
        {
            _DataTypeConverter = dataTypeConverter ?? throw new ArgumentNullException(nameof(dataTypeConverter));
            _BaseColumnMappings = baseColumnMappings ?? throw new ArgumentNullException(nameof(baseColumnMappings));
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _ProcessedEntities = new Dictionary<string, HashSet<object>>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Maps a MySQL data reader to a list of entities, handling joined results and navigation properties.
        /// </summary>
        /// <param name="reader">The MySQL data reader containing the query results</param>
        /// <param name="joinResult">Join metadata for mapping related entities</param>
        /// <param name="includes">Navigation property information for Include operations</param>
        /// <returns>A list of mapped entities with populated navigation properties</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public List<T> MapJoinedResults(
            MySqlDataReader reader,
            MySqlJoinBuilder.MySqlJoinResult joinResult,
            List<MySqlIncludeInfo> includes)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            Dictionary<object, T> primaryEntities = new Dictionary<object, T>();
            List<T> results = new List<T>();

            while (reader.Read())
            {
                try
                {
                    T primaryEntity = MapPrimaryEntity(reader, primaryEntities);

                    object entityKey = GetEntityKey(primaryEntity);
                    if (!primaryEntities.ContainsKey(entityKey))
                    {
                        primaryEntities[entityKey] = primaryEntity;
                        results.Add(primaryEntity);
                    }
                    else
                    {
                        primaryEntity = primaryEntities[entityKey];
                    }

                    if (includes != null && includes.Count > 0)
                    {
                        MapRelatedEntities(reader, primaryEntity, includes, joinResult);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping entity from data reader at row {results.Count + 1}", ex);
                }
            }

            return results;
        }

        /// <summary>
        /// Maps a simple MySQL data reader to a list of entities without joins or navigation properties.
        /// Provides enhanced type conversion compared to basic mapping implementations.
        /// </summary>
        /// <param name="reader">The MySQL data reader containing the query results</param>
        /// <returns>A list of mapped entities</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public List<T> MapSimpleResults(MySqlDataReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            List<T> results = new List<T>();

            while (reader.Read())
            {
                try
                {
                    T entity = MapSingleEntity(reader, _BaseColumnMappings);
                    results.Add(entity);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping entity from data reader at row {results.Count + 1}", ex);
                }
            }

            return results;
        }

        /// <summary>
        /// Maps a MySQL data reader to a list of entities asynchronously, handling joined results and navigation properties.
        /// </summary>
        /// <param name="reader">The MySQL data reader containing the query results</param>
        /// <param name="joinResult">Join metadata for mapping related entities</param>
        /// <param name="includes">Navigation property information for Include operations</param>
        /// <param name="cancellationToken">Cancellation token for the async operation</param>
        /// <returns>A list of mapped entities with populated navigation properties</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public async Task<List<T>> MapJoinedResultsAsync(
            MySqlDataReader reader,
            MySqlJoinBuilder.MySqlJoinResult joinResult,
            List<MySqlIncludeInfo> includes,
            CancellationToken cancellationToken = default)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            Dictionary<object, T> primaryEntities = new Dictionary<object, T>();
            List<T> results = new List<T>();

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    T primaryEntity = MapPrimaryEntity(reader, primaryEntities);

                    object entityKey = GetEntityKey(primaryEntity);
                    if (!primaryEntities.ContainsKey(entityKey))
                    {
                        primaryEntities[entityKey] = primaryEntity;
                        results.Add(primaryEntity);
                    }
                    else
                    {
                        primaryEntity = primaryEntities[entityKey];
                    }

                    if (includes != null && includes.Count > 0)
                    {
                        MapRelatedEntities(reader, primaryEntity, includes, joinResult);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping entity from data reader at row {results.Count + 1}", ex);
                }
            }

            return results;
        }

        /// <summary>
        /// Maps a simple MySQL data reader to a list of entities asynchronously without joins or navigation properties.
        /// Provides enhanced type conversion compared to basic mapping implementations.
        /// </summary>
        /// <param name="reader">The MySQL data reader containing the query results</param>
        /// <param name="cancellationToken">Cancellation token for the async operation</param>
        /// <returns>A list of mapped entities</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public async Task<List<T>> MapSimpleResultsAsync(
            MySqlDataReader reader,
            CancellationToken cancellationToken = default)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            List<T> results = new List<T>();

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    T entity = MapSingleEntity(reader, _BaseColumnMappings);
                    results.Add(entity);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping entity from data reader at row {results.Count + 1}", ex);
                }
            }

            return results;
        }

        /// <summary>
        /// Clears the internal entity processing cache. Call this between different query executions
        /// to prevent issues with entity identity tracking.
        /// </summary>
        public void ClearProcessingCache()
        {
            _ProcessedEntities.Clear();
        }

        #endregion

        #region Private-Methods

        private T MapPrimaryEntity(MySqlDataReader reader, Dictionary<object, T> existingEntities)
        {
            T entity = MapSingleEntity(reader, _BaseColumnMappings);

            // Check if we've already processed this entity
            object entityKey = GetEntityKey(entity);
            if (existingEntities.ContainsKey(entityKey))
            {
                return existingEntities[entityKey];
            }

            return entity;
        }

        private T MapSingleEntity(MySqlDataReader reader, Dictionary<string, PropertyInfo> columnMappings)
        {
            T entity = new T();

            foreach (KeyValuePair<string, PropertyInfo> kvp in columnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                try
                {
                    if (HasColumn(reader, columnName))
                    {
                        int ordinal = reader.GetOrdinal(columnName);
                        if (!reader.IsDBNull(ordinal))
                        {
                            object value = reader.GetValue(ordinal);
                            object convertedValue = ConvertDatabaseValue(value, property);
                            property.SetValue(entity, convertedValue);
                        }
                        else
                        {
                            // Handle nullable properties
                            HandleNullValue(entity, property);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping column '{columnName}' to property '{property.Name}' on type '{typeof(T).Name}'", ex);
                }
            }

            return entity;
        }

        private void MapRelatedEntities(
            MySqlDataReader reader,
            T primaryEntity,
            List<MySqlIncludeInfo> includes,
            MySqlJoinBuilder.MySqlJoinResult joinResult)
        {
            foreach (MySqlIncludeInfo include in includes)
            {
                try
                {
                    if (include.IsCollection)
                    {
                        // Collection navigation properties require special handling
                        MapCollectionNavigationProperty(reader, primaryEntity, include, joinResult);
                    }
                    else
                    {
                        // Single navigation property
                        MapSingleNavigationProperty(reader, primaryEntity, include, joinResult);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping navigation property '{include.PropertyPath}' on entity '{typeof(T).Name}'", ex);
                }
            }
        }

        private void MapSingleNavigationProperty(
            MySqlDataReader reader,
            T primaryEntity,
            MySqlIncludeInfo include,
            MySqlJoinBuilder.MySqlJoinResult joinResult)
        {
            object? relatedEntity = MapRelatedEntity(reader, include, joinResult);

            if (relatedEntity != null)
            {
                include.NavigationProperty.SetValue(primaryEntity, relatedEntity);

                // Handle nested includes (ThenInclude scenarios)
                if (include.Children.Count > 0)
                {
                    MapNestedRelatedEntities(reader, relatedEntity, include.Children, joinResult);
                }
            }
        }

        private void MapCollectionNavigationProperty(
            MySqlDataReader reader,
            T primaryEntity,
            MySqlIncludeInfo include,
            MySqlJoinBuilder.MySqlJoinResult joinResult)
        {
            object? relatedEntity = MapRelatedEntity(reader, include, joinResult);
            if (relatedEntity == null)
                return;

            // Get the entity key for deduplication
            object relatedEntityKey = GetEntityKeyForType(relatedEntity, include.RelatedEntityType);
            string cacheKey = $"{include.PropertyPath}_{GetEntityKey(primaryEntity)}";

            // Track processed entities for this navigation property
            if (!_ProcessedEntities.ContainsKey(cacheKey))
            {
                _ProcessedEntities[cacheKey] = new HashSet<object>();
            }

            // Skip if we've already processed this related entity for this primary entity
            if (_ProcessedEntities[cacheKey].Contains(relatedEntityKey))
            {
                return;
            }

            _ProcessedEntities[cacheKey].Add(relatedEntityKey);

            // Get or create the collection
            object? existingCollection = include.NavigationProperty.GetValue(primaryEntity);
            if (existingCollection == null)
            {
                Type collectionType = include.NavigationProperty.PropertyType;
                if (collectionType.IsGenericType)
                {
                    Type genericDefinition = collectionType.GetGenericTypeDefinition();
                    if (genericDefinition == typeof(List<>) ||
                        genericDefinition == typeof(IList<>) ||
                        genericDefinition == typeof(ICollection<>) ||
                        genericDefinition == typeof(IEnumerable<>))
                    {
                        Type elementType = collectionType.GetGenericArguments()[0];
                        Type listType = typeof(List<>).MakeGenericType(elementType);
                        existingCollection = Activator.CreateInstance(listType);
                        include.NavigationProperty.SetValue(primaryEntity, existingCollection);
                    }
                }
            }

            // Add the related entity to the collection
            if (existingCollection is System.Collections.IList list)
            {
                list.Add(relatedEntity);

                // Handle nested includes for collection items
                if (include.Children.Count > 0)
                {
                    MapNestedRelatedEntities(reader, relatedEntity, include.Children, joinResult);
                }
            }
        }

        private void MapNestedRelatedEntities(
            MySqlDataReader reader,
            object parentEntity,
            List<MySqlIncludeInfo> includes,
            MySqlJoinBuilder.MySqlJoinResult joinResult)
        {
            foreach (MySqlIncludeInfo include in includes)
            {
                try
                {
                    if (include.IsCollection)
                    {
                        // Handle collection navigation properties in nested scenarios
                        MapNestedCollectionNavigationProperty(reader, parentEntity, include, joinResult);
                    }
                    else
                    {
                        // Handle single navigation properties
                        object? relatedEntity = MapRelatedEntity(reader, include, joinResult);

                        if (relatedEntity != null)
                        {
                            include.NavigationProperty.SetValue(parentEntity, relatedEntity);

                            if (include.Children.Count > 0)
                            {
                                MapNestedRelatedEntities(reader, relatedEntity, include.Children, joinResult);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping nested navigation property '{include.PropertyPath}' on entity '{parentEntity.GetType().Name}'", ex);
                }
            }
        }

        private object? MapRelatedEntity(
            MySqlDataReader reader,
            MySqlIncludeInfo include,
            MySqlJoinBuilder.MySqlJoinResult joinResult)
        {
            if (joinResult?.ColumnMappingsByAlias == null || !joinResult.ColumnMappingsByAlias.ContainsKey(include.JoinAlias))
            {
                return null;
            }

            List<MySqlJoinBuilder.MySqlColumnMapping> mappings = joinResult.ColumnMappingsByAlias[include.JoinAlias];
            object entity = Activator.CreateInstance(include.RelatedEntityType)!;

            bool hasAnyValue = false;

            foreach (MySqlJoinBuilder.MySqlColumnMapping mapping in mappings)
            {
                try
                {
                    string columnName = mapping.Alias ?? mapping.ColumnName;
                    if (HasColumn(reader, columnName))
                    {
                        int ordinal = reader.GetOrdinal(columnName);

                        if (!reader.IsDBNull(ordinal))
                        {
                            hasAnyValue = true;
                            object value = reader.GetValue(ordinal);
                            object convertedValue = ConvertDatabaseValue(value, mapping.Property);
                            mapping.Property.SetValue(entity, convertedValue);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping related entity column '{mapping.ColumnName}' to property '{mapping.Property.Name}'", ex);
                }
            }

            return hasAnyValue ? entity : null;
        }

        private object ConvertDatabaseValue(object value, PropertyInfo property)
        {
            try
            {
                // Use the advanced data type converter for complex conversions
                return _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to convert database value '{value}' of type '{value.GetType().Name}' to property type '{property.PropertyType.Name}'", ex);
            }
        }

        private void HandleNullValue(T entity, PropertyInfo property)
        {
            // For nullable value types and reference types, null is fine
            if (property.PropertyType.IsValueType && !IsNullableValueType(property.PropertyType))
            {
                // For non-nullable value types, set to default value
                object defaultValue = Activator.CreateInstance(property.PropertyType)!;
                property.SetValue(entity, defaultValue);
            }
            // For reference types and nullable value types, leaving as null is correct
        }

        private bool IsNullableValueType(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        private object GetEntityKey(T entity)
        {
            // Find the primary key property and use its value as the entity key
            foreach (KeyValuePair<string, PropertyInfo> kvp in _BaseColumnMappings)
            {
                PropertyAttribute? attr = kvp.Value.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    object? keyValue = kvp.Value.GetValue(entity);
                    return keyValue ?? entity.GetHashCode();
                }
            }

            // Fallback to hash code if no primary key found
            return entity.GetHashCode();
        }

        private bool HasColumn(MySqlDataReader reader, string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        private void MapNestedCollectionNavigationProperty(
            MySqlDataReader reader,
            object parentEntity,
            MySqlIncludeInfo include,
            MySqlJoinBuilder.MySqlJoinResult joinResult)
        {
            object? relatedEntity = MapRelatedEntity(reader, include, joinResult);
            if (relatedEntity == null)
                return;

            // Get the entity key for deduplication
            object relatedEntityKey = GetEntityKeyForType(relatedEntity, include.RelatedEntityType);
            object parentEntityKey = GetEntityKeyForType(parentEntity, parentEntity.GetType());
            string cacheKey = $"{include.PropertyPath}_{parentEntityKey}";

            // Track processed entities for this navigation property
            if (!_ProcessedEntities.ContainsKey(cacheKey))
            {
                _ProcessedEntities[cacheKey] = new HashSet<object>();
            }

            // Skip if we've already processed this related entity for this parent entity
            if (_ProcessedEntities[cacheKey].Contains(relatedEntityKey))
            {
                return;
            }

            _ProcessedEntities[cacheKey].Add(relatedEntityKey);

            // Get or create the collection
            object? existingCollection = include.NavigationProperty.GetValue(parentEntity);
            if (existingCollection == null)
            {
                Type collectionType = include.NavigationProperty.PropertyType;
                if (collectionType.IsGenericType)
                {
                    Type genericDefinition = collectionType.GetGenericTypeDefinition();
                    if (genericDefinition == typeof(List<>) ||
                        genericDefinition == typeof(IList<>) ||
                        genericDefinition == typeof(ICollection<>) ||
                        genericDefinition == typeof(IEnumerable<>))
                    {
                        Type elementType = collectionType.GetGenericArguments()[0];
                        Type listType = typeof(List<>).MakeGenericType(elementType);
                        existingCollection = Activator.CreateInstance(listType);
                        include.NavigationProperty.SetValue(parentEntity, existingCollection);
                    }
                }
            }

            // Add the related entity to the collection
            if (existingCollection is System.Collections.IList list)
            {
                list.Add(relatedEntity);

                // Handle nested includes for collection items
                if (include.Children.Count > 0)
                {
                    MapNestedRelatedEntities(reader, relatedEntity, include.Children, joinResult);
                }
            }
        }

        private object GetEntityKeyForType(object entity, Type entityType)
        {
            // Find the primary key property for the specified type
            foreach (PropertyInfo prop in entityType.GetProperties())
            {
                PropertyAttribute? attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    object? keyValue = prop.GetValue(entity);
                    return keyValue ?? entity.GetHashCode();
                }
            }

            // Fallback to hash code if no primary key found
            return entity.GetHashCode();
        }

        #endregion
    }
}