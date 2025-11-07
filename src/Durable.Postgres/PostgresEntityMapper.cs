namespace Durable.Postgres
{
    using Npgsql;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Advanced entity mapping capabilities for PostgreSQL data readers.
    /// Handles complex type conversions, navigation properties, and relationship mapping
    /// with support for Include operations and nested entity graphs.
    /// </summary>
    /// <typeparam name="T">The primary entity type being mapped</typeparam>
    internal class PostgresEntityMapper<T> where T : class, new()
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
        /// Initializes a new instance of the PostgresEntityMapper class.
        /// </summary>
        /// <param name="dataTypeConverter">The data type converter for handling complex type conversions</param>
        /// <param name="baseColumnMappings">Column mappings for the primary entity type</param>
        /// <param name="sanitizer">The sanitizer for handling SQL values and identifiers</param>
        /// <exception cref="ArgumentNullException">Thrown when any required parameter is null</exception>
        public PostgresEntityMapper(
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
        /// Maps a PostgreSQL data reader to a list of entities, handling joined results and navigation properties.
        /// </summary>
        /// <param name="reader">The PostgreSQL data reader containing the query results</param>
        /// <param name="joinResult">Join metadata for mapping related entities</param>
        /// <param name="includes">Navigation property information for Include operations</param>
        /// <returns>A list of mapped entities with populated navigation properties</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public List<T> MapJoinedResults(
            NpgsqlDataReader reader,
            PostgresJoinBuilder.PostgresJoinResult joinResult,
            List<PostgresIncludeInfo> includes)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            Dictionary<object, T> primaryEntities = new Dictionary<object, T>();
            List<T> results = new List<T>();

            // Get the base table column mappings from joinResult (with aliases)
            Dictionary<string, PropertyInfo> baseColumnMappings = new Dictionary<string, PropertyInfo>();
            if (joinResult.ColumnMappingsByAlias.ContainsKey("t0"))
            {
                foreach (PostgresJoinBuilder.PostgresColumnMapping mapping in joinResult.ColumnMappingsByAlias["t0"])
                {
                    baseColumnMappings[mapping.ColumnName] = mapping.Property;
                }
            }
            else
            {
                baseColumnMappings = _BaseColumnMappings;
            }

            while (reader.Read())
            {
                try
                {
                    T primaryEntity = MapSingleEntity(reader, baseColumnMappings);

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
        /// Maps a simple PostgreSQL data reader to a list of entities without joins or navigation properties.
        /// Provides enhanced type conversion compared to basic mapping implementations.
        /// </summary>
        /// <param name="reader">The PostgreSQL data reader containing the query results</param>
        /// <returns>A list of mapped entities</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public List<T> MapSimpleResults(NpgsqlDataReader reader)
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
        /// Asynchronously maps a simple PostgreSQL data reader to a list of entities without joins or navigation properties.
        /// Provides enhanced type conversion compared to basic mapping implementations.
        /// </summary>
        /// <param name="reader">The PostgreSQL data reader containing the query results</param>
        /// <param name="cancellationToken">Cancellation token for the async operation</param>
        /// <returns>A list of mapped entities</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public async Task<List<T>> MapSimpleResultsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken = default)
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
        /// Maps a single entity from the current row of a PostgreSQL data reader.
        /// This is a simplified version for basic query operations without joins.
        /// </summary>
        /// <param name="reader">The PostgreSQL data reader positioned at the desired row</param>
        /// <param name="joinResult">Optional join result for complex mapping scenarios</param>
        /// <returns>A mapped entity of type T</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public T MapEntity(NpgsqlDataReader reader, PostgresJoinBuilder.PostgresJoinResult? joinResult = null)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            try
            {
                return MapSingleEntity(reader, _BaseColumnMappings);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Error mapping entity from data reader", ex);
            }
        }

        /// <summary>
        /// Maps a PostgreSQL data reader to a list of entities asynchronously, handling joined results and navigation properties.
        /// </summary>
        /// <param name="reader">The PostgreSQL data reader containing the query results</param>
        /// <param name="joinResult">Join metadata for mapping related entities</param>
        /// <param name="includes">Navigation property information for Include operations</param>
        /// <param name="cancellationToken">Cancellation token for the async operation</param>
        /// <returns>A list of mapped entities with populated navigation properties</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when mapping encounters errors</exception>
        public async Task<List<T>> MapJoinedResultsAsync(
            NpgsqlDataReader reader,
            PostgresJoinBuilder.PostgresJoinResult joinResult,
            List<PostgresIncludeInfo> includes,
            CancellationToken cancellationToken = default)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            Dictionary<object, T> primaryEntities = new Dictionary<object, T>();
            List<T> results = new List<T>();

            // Get the base table column mappings from joinResult (with aliases)
            Dictionary<string, PropertyInfo> baseColumnMappings = new Dictionary<string, PropertyInfo>();
            if (joinResult.ColumnMappingsByAlias.ContainsKey("t0"))
            {
                foreach (PostgresJoinBuilder.PostgresColumnMapping mapping in joinResult.ColumnMappingsByAlias["t0"])
                {
                    baseColumnMappings[mapping.ColumnName] = mapping.Property;
                }
            }
            else
            {
                baseColumnMappings = _BaseColumnMappings;
            }

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    T primaryEntity = MapSingleEntity(reader, baseColumnMappings);

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

        #endregion

        #region Private-Methods

        /// <summary>
        /// Maps a single entity from the data reader using the provided column mappings.
        /// </summary>
        /// <param name="reader">The data reader to read from</param>
        /// <param name="columnMappings">The column to property mappings</param>
        /// <returns>A mapped entity</returns>
        private T MapSingleEntity(NpgsqlDataReader reader, Dictionary<string, PropertyInfo> columnMappings)
        {
            T entity = new T();

            foreach (KeyValuePair<string, PropertyInfo> mapping in columnMappings)
            {
                string columnName = mapping.Key;
                PropertyInfo property = mapping.Value;

                try
                {
                    // Check if column exists in the result set
                    int columnIndex = GetColumnIndex(reader, columnName);
                    if (columnIndex == -1) continue; // Column not found, skip

                    object? value = reader.IsDBNull(columnIndex) ? null : reader.GetValue(columnIndex);

                    if (value != null)
                    {
                        // Convert PostgreSQL-specific types
                        object? convertedValue = ConvertPostgresValue(value, property.PropertyType);
                        property.SetValue(entity, convertedValue);
                    }
                    else if (property.PropertyType.IsValueType && Nullable.GetUnderlyingType(property.PropertyType) == null)
                    {
                        // Set default value for non-nullable value types
                        property.SetValue(entity, Activator.CreateInstance(property.PropertyType));
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping column '{columnName}' to property '{property.Name}'", ex);
                }
            }

            return entity;
        }

        /// <summary>
        /// Converts PostgreSQL-specific values to appropriate .NET types.
        /// Handles PostgreSQL arrays, JSON/JSONB, UUID, timestamps, geometric types, and other PostgreSQL-specific data types.
        /// </summary>
        /// <param name="value">The raw value from PostgreSQL</param>
        /// <param name="targetType">The target .NET type</param>
        /// <returns>The converted value</returns>
        private object? ConvertPostgresValue(object? value, Type targetType)
        {
            if (value == null) return null;

            Type actualTargetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            // Handle PostgreSQL UUID type
            if (actualTargetType == typeof(Guid))
            {
                if (value is Guid guidValue)
                    return guidValue;
                if (value is string stringValue)
                    return Guid.Parse(stringValue);
            }

            // Handle PostgreSQL timestamp types
            if (actualTargetType == typeof(DateTime))
            {
                if (value is DateTime dateTimeValue)
                {
                    // PostgreSQL timestamptz returns DateTimeOffset, timestamp returns DateTime
                    return DateTime.SpecifyKind(dateTimeValue, DateTimeKind.Utc);
                }
                if (value is DateTimeOffset dateTimeOffsetValue)
                {
                    return dateTimeOffsetValue.DateTime;
                }
            }

            if (actualTargetType == typeof(DateTimeOffset))
            {
                if (value is DateTimeOffset dateTimeOffsetValue)
                    return dateTimeOffsetValue;
                if (value is DateTime dateTimeValue)
                    return new DateTimeOffset(DateTime.SpecifyKind(dateTimeValue, DateTimeKind.Utc));
            }

            // Handle PostgreSQL JSON and JSONB types
            if (actualTargetType == typeof(string) && (value is System.Text.Json.JsonElement || value.GetType().Name.Contains("Json")))
            {
                return value.ToString()!;
            }

            // Handle PostgreSQL arrays (including multi-dimensional arrays)
            if (actualTargetType.IsArray && value is Array arrayValue)
            {
                Type elementType = actualTargetType.GetElementType()!;
                Array result = Array.CreateInstance(elementType, arrayValue.Length);
                for (int i = 0; i < arrayValue.Length; i++)
                {
                    object? element = arrayValue.GetValue(i);
                    result.SetValue(ConvertPostgresValue(element!, elementType), i);
                }
                return result;
            }

            // Handle PostgreSQL arrays that come as object arrays
            if (actualTargetType.IsArray && value is object[] objectArrayValue)
            {
                Type elementType = actualTargetType.GetElementType()!;
                Array result = Array.CreateInstance(elementType, objectArrayValue.Length);
                for (int i = 0; i < objectArrayValue.Length; i++)
                {
                    result.SetValue(ConvertPostgresValue(objectArrayValue[i], elementType), i);
                }
                return result;
            }

            // Handle PostgreSQL geometric types (point, line, box, circle, etc.)
            if (value.GetType().Namespace?.Contains("NpgsqlTypes") == true)
            {
                // For PostgreSQL geometric types, convert to string representation
                // or handle specific types if the target application uses them
                if (actualTargetType == typeof(string))
                    return value.ToString()!;
            }

            // Handle PostgreSQL bit strings
            if (actualTargetType == typeof(byte[]) && value.GetType().Name.Contains("BitArray"))
            {
                // Convert BitArray to byte array if needed
                if (value is System.Collections.BitArray bitArray)
                {
                    byte[] bytes = new byte[(bitArray.Length + 7) / 8];
                    bitArray.CopyTo(bytes, 0);
                    return bytes;
                }
            }

            // Handle PostgreSQL network address types (inet, cidr, macaddr)
            if (actualTargetType == typeof(System.Net.IPAddress) && value is string ipString)
            {
                if (System.Net.IPAddress.TryParse(ipString, out System.Net.IPAddress? ipAddress))
                    return ipAddress;
            }

            // Handle PostgreSQL ranges
            if (value.GetType().Name.Contains("Range"))
            {
                if (actualTargetType == typeof(string))
                    return value.ToString()!;
            }

            // Use the data type converter for standard conversions
            return _DataTypeConverter.ConvertFromDatabase(value, actualTargetType)!;
        }

        /// <summary>
        /// Gets the column index for the specified column name, case-insensitive.
        /// </summary>
        /// <param name="reader">The data reader</param>
        /// <param name="columnName">The column name to find</param>
        /// <returns>The column index, or -1 if not found</returns>
        private int GetColumnIndex(NpgsqlDataReader reader, string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (string.Equals(reader.GetName(i), columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// Maps the primary entity from the current reader row.
        /// </summary>
        /// <param name="reader">The data reader</param>
        /// <param name="primaryEntities">Existing primary entities cache</param>
        /// <returns>The mapped primary entity</returns>
        private T MapPrimaryEntity(NpgsqlDataReader reader, Dictionary<object, T> primaryEntities)
        {
            return MapSingleEntity(reader, _BaseColumnMappings);
        }

        /// <summary>
        /// Gets a unique key for the entity (typically primary key value).
        /// </summary>
        /// <param name="entity">The entity to get the key for</param>
        /// <returns>A unique key for the entity</returns>
        private object GetEntityKey(T entity)
        {
            // Find the primary key property and use its value as the entity key
            foreach (KeyValuePair<string, PropertyInfo> kvp in _BaseColumnMappings)
            {
                PropertyAttribute? attr = kvp.Value.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    object? keyValue = kvp.Value.GetValue(entity);
                    if (keyValue != null)
                    {
                        // For value types, return the value directly to ensure proper dictionary equality
                        return keyValue;
                    }
                    // If primary key is null, use entity hash code
                    return entity.GetHashCode();
                }
            }

            // Fallback to hash code if no primary key found
            return entity.GetHashCode();
        }

        /// <summary>
        /// Maps related entities from joined results.
        /// </summary>
        /// <param name="reader">The data reader</param>
        /// <param name="primaryEntity">The primary entity</param>
        /// <param name="includes">Include information</param>
        /// <param name="joinResult">Join result metadata</param>
        private void MapRelatedEntities(NpgsqlDataReader reader, T primaryEntity, List<PostgresIncludeInfo> includes, PostgresJoinBuilder.PostgresJoinResult joinResult)
        {
            foreach (PostgresIncludeInfo include in includes)
            {
                try
                {
                    if (include.IsCollection)
                    {
                        MapCollectionNavigationProperty(reader, primaryEntity, include, joinResult);
                    }
                    else
                    {
                        MapSingleNavigationProperty(reader, primaryEntity, include, joinResult);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error mapping navigation property '{include.PropertyPath}' on entity '{typeof(T).Name}'", ex);
                }
            }
        }

        private void MapSingleNavigationProperty(NpgsqlDataReader reader, T primaryEntity, PostgresIncludeInfo include, PostgresJoinBuilder.PostgresJoinResult joinResult)
        {
            object? relatedEntity = MapRelatedEntity(reader, include, joinResult);
            if (relatedEntity != null)
            {
                include.NavigationProperty.SetValue(primaryEntity, relatedEntity);
            }
        }

        private void MapCollectionNavigationProperty(NpgsqlDataReader reader, T primaryEntity, PostgresIncludeInfo include, PostgresJoinBuilder.PostgresJoinResult joinResult)
        {
            object? relatedEntity = MapRelatedEntity(reader, include, joinResult);
            if (relatedEntity == null)
                return;

            object relatedEntityKey = GetEntityKeyForType(relatedEntity, include.RelatedEntityType);
            string cacheKey = $"{include.PropertyPath}_{GetEntityKey(primaryEntity)}";

            if (!_ProcessedEntities.ContainsKey(cacheKey))
            {
                _ProcessedEntities[cacheKey] = new HashSet<object>();
            }

            if (_ProcessedEntities[cacheKey].Contains(relatedEntityKey))
            {
                return;
            }

            _ProcessedEntities[cacheKey].Add(relatedEntityKey);

            object? existingCollection = include.NavigationProperty.GetValue(primaryEntity);
            if (existingCollection == null)
            {
                Type collectionType = typeof(List<>).MakeGenericType(include.RelatedEntityType);
                existingCollection = Activator.CreateInstance(collectionType)!;
                include.NavigationProperty.SetValue(primaryEntity, existingCollection);
            }

            System.Collections.IList collection = (System.Collections.IList)existingCollection;
            collection.Add(relatedEntity);
        }

        private object? MapRelatedEntity(NpgsqlDataReader reader, PostgresIncludeInfo include, PostgresJoinBuilder.PostgresJoinResult joinResult)
        {
            if (!joinResult.ColumnMappingsByAlias.ContainsKey(include.JoinAlias))
                return null;

            List<PostgresJoinBuilder.PostgresColumnMapping> columnMappings = joinResult.ColumnMappingsByAlias[include.JoinAlias];
            Dictionary<string, PropertyInfo> mappingsDict = new Dictionary<string, PropertyInfo>();
            foreach (PostgresJoinBuilder.PostgresColumnMapping mapping in columnMappings)
            {
                mappingsDict[mapping.ColumnName] = mapping.Property;
            }

            bool hasNonNullValue = false;
            foreach (PostgresJoinBuilder.PostgresColumnMapping mapping in columnMappings)
            {
                string columnName = mapping.Alias ?? mapping.ColumnName;
                int columnIndex = GetColumnIndex(reader, columnName);
                if (columnIndex != -1 && !reader.IsDBNull(columnIndex))
                {
                    hasNonNullValue = true;
                    break;
                }
            }

            if (!hasNonNullValue)
                return null;

            object relatedEntity = Activator.CreateInstance(include.RelatedEntityType)!;
            foreach (PostgresJoinBuilder.PostgresColumnMapping mapping in columnMappings)
            {
                string columnName = mapping.Alias ?? mapping.ColumnName;
                int columnIndex = GetColumnIndex(reader, columnName);
                if (columnIndex != -1 && !reader.IsDBNull(columnIndex))
                {
                    object value = reader.GetValue(columnIndex);
                    object? convertedValue = ConvertPostgresValue(value, mapping.Property.PropertyType);
                    mapping.Property.SetValue(relatedEntity, convertedValue);
                }
            }

            return relatedEntity;
        }

        private object GetEntityKeyForType(object entity, Type entityType)
        {
            PropertyInfo? idProperty = entityType.GetProperty("Id");
            if (idProperty != null)
            {
                object? value = idProperty.GetValue(entity);
                return value ?? Guid.NewGuid();
            }
            return entity;
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
    }
}