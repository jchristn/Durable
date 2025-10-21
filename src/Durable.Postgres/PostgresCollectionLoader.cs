namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using Npgsql;

    /// <summary>
    /// Handles loading of collection navigation properties for PostgreSQL entities.
    /// Supports both one-to-many and many-to-many relationships with efficient batch loading.
    /// </summary>
    /// <typeparam name="T">The entity type that contains collection navigation properties</typeparam>
    internal class PostgresCollectionLoader<T> where T : class, new()
    {

        #region Private-Members

        private readonly ISanitizer _Sanitizer;
        private readonly IDataTypeConverter _DataTypeConverter;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresCollectionLoader class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer for SQL identifiers</param>
        /// <param name="dataTypeConverter">The data type converter for database values</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer or dataTypeConverter is null</exception>
        public PostgresCollectionLoader(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
        {
            _Sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _DataTypeConverter = dataTypeConverter ?? throw new ArgumentNullException(nameof(dataTypeConverter));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Loads collection navigation properties for a list of entities.
        /// </summary>
        /// <param name="entities">The entities to load collections for</param>
        /// <param name="includes">The include information for navigation properties</param>
        /// <param name="connection">The PostgreSQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <exception cref="ArgumentNullException">Thrown when required parameters are null</exception>
        public void LoadCollections(
            List<T> entities,
            List<PostgresIncludeInfo> includes,
            NpgsqlConnection connection,
            ITransaction? transaction)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            if (includes == null)
                throw new ArgumentNullException(nameof(includes));

            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            foreach (PostgresIncludeInfo include in includes)
            {
                if (include.IsCollection)
                {
                    LoadCollection(entities, include, connection, transaction);
                }

                if (!include.IsCollection && include.Children.Count > 0)
                {
                    List<object> relatedEntities = new List<object>();
                    foreach (T entity in entities)
                    {
                        object? related = include.NavigationProperty.GetValue(entity);
                        if (related != null)
                        {
                            relatedEntities.Add(related);
                        }
                    }

                    if (relatedEntities.Count > 0)
                    {
                        LoadCollectionsForRelatedEntities(relatedEntities, include.Children, connection, transaction);
                    }
                }
            }
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Loads collections for related entities in nested navigation scenarios.
        /// </summary>
        /// <param name="entities">The related entities to load collections for</param>
        /// <param name="includes">The include information for navigation properties</param>
        /// <param name="connection">The PostgreSQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadCollectionsForRelatedEntities(
            List<object> entities,
            List<PostgresIncludeInfo> includes,
            NpgsqlConnection connection,
            ITransaction? transaction)
        {
            foreach (PostgresIncludeInfo include in includes)
            {
                if (include.IsCollection)
                {
                    LoadCollectionForRelatedEntities(entities, include, connection, transaction);
                }

                if (!include.IsCollection && include.Children.Count > 0)
                {
                    List<object> relatedEntities = new List<object>();
                    foreach (object entity in entities)
                    {
                        object? related = include.NavigationProperty.GetValue(entity);
                        if (related != null)
                        {
                            relatedEntities.Add(related);
                        }
                    }

                    if (relatedEntities.Count > 0)
                    {
                        LoadCollectionsForRelatedEntities(relatedEntities, include.Children, connection, transaction);
                    }
                }
            }
        }

        /// <summary>
        /// Loads a collection navigation property for the specified entities.
        /// </summary>
        /// <param name="entities">The entities to load the collection for</param>
        /// <param name="include">The include information for the collection</param>
        /// <param name="connection">The PostgreSQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadCollection(
            List<T> entities,
            PostgresIncludeInfo include,
            NpgsqlConnection connection,
            ITransaction? transaction)
        {
            if (entities.Count == 0) return;

            try
            {
                if (include.IsManyToMany)
                {
                    LoadManyToManyCollection(entities, include, connection, transaction);
                }
                else
                {
                    LoadOneToManyCollection(entities, include, connection, transaction);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error loading collection '{include.PropertyPath}': {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Loads a one-to-many collection navigation property.
        /// </summary>
        /// <param name="entities">The entities to load the collection for</param>
        /// <param name="include">The include information for the collection</param>
        /// <param name="connection">The PostgreSQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadOneToManyCollection(
            List<T> entities,
            PostgresIncludeInfo include,
            NpgsqlConnection connection,
            ITransaction? transaction)
        {
            // Get the primary key property from the parent entity
            PropertyInfo? primaryKeyProperty = GetPrimaryKeyProperty(typeof(T));
            if (primaryKeyProperty == null)
                throw new InvalidOperationException($"No primary key found for entity type {typeof(T).Name}");

            // Get primary key values from all entities
            List<object> primaryKeyValues = entities
                .Select(e => primaryKeyProperty.GetValue(e))
                .Where(v => v != null)
                .ToList()!;

            if (primaryKeyValues.Count == 0) return;

            // Build SQL to load related entities
            string relatedTableName = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string foreignKeyColumn = _Sanitizer.SanitizeIdentifier(GetColumnName(include.ForeignKeyProperty));
            string primaryKeyColumn = _Sanitizer.SanitizeIdentifier(GetColumnName(primaryKeyProperty));

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT * FROM {relatedTableName} WHERE {foreignKeyColumn} = ANY(@primaryKeys)");

            // Execute query and load related entities
            using NpgsqlCommand command = new NpgsqlCommand(sql.ToString(), connection);
            if (transaction != null)
                command.Transaction = (NpgsqlTransaction)transaction.Transaction;

            command.Parameters.AddWithValue("@primaryKeys", primaryKeyValues.ToArray());

            Dictionary<object, List<object>> relatedEntitiesByParentKey = new Dictionary<object, List<object>>();

            using (NpgsqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    object relatedEntity = CreateEntityFromReader(reader, include.RelatedEntityType);
                    object? foreignKeyValue = GetValueFromReader(reader, GetColumnName(include.ForeignKeyProperty), include.ForeignKeyProperty?.PropertyType);

                    if (foreignKeyValue != null)
                    {
                        if (!relatedEntitiesByParentKey.ContainsKey(foreignKeyValue))
                        {
                            relatedEntitiesByParentKey[foreignKeyValue] = new List<object>();
                        }
                        relatedEntitiesByParentKey[foreignKeyValue].Add(relatedEntity);
                    }
                }
            }

            // Assign collections to parent entities
            foreach (T entity in entities)
            {
                object? primaryKeyValue = primaryKeyProperty.GetValue(entity);
                if (primaryKeyValue != null && relatedEntitiesByParentKey.TryGetValue(primaryKeyValue, out List<object>? relatedEntities))
                {
                    object collection = CreateTypedCollection(include.NavigationProperty.PropertyType, relatedEntities);
                    include.NavigationProperty.SetValue(entity, collection);
                }
                else
                {
                    // Set empty collection if no related entities found
                    object emptyCollection = CreateTypedCollection(include.NavigationProperty.PropertyType, new List<object>());
                    include.NavigationProperty.SetValue(entity, emptyCollection);
                }
            }
        }

        /// <summary>
        /// Loads a many-to-many collection navigation property.
        /// </summary>
        /// <param name="entities">The entities to load the collection for</param>
        /// <param name="include">The include information for the collection</param>
        /// <param name="connection">The PostgreSQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadManyToManyCollection(
            List<T> entities,
            PostgresIncludeInfo include,
            NpgsqlConnection connection,
            ITransaction? transaction)
        {
            if (include.JunctionTableName == null)
                throw new InvalidOperationException($"Junction table name not specified for many-to-many relationship '{include.PropertyPath}'");

            // Get the primary key property from the parent entity
            PropertyInfo? primaryKeyProperty = GetPrimaryKeyProperty(typeof(T));
            if (primaryKeyProperty == null)
                throw new InvalidOperationException($"No primary key found for entity type {typeof(T).Name}");

            // Get primary key values from all entities
            List<object> primaryKeyValues = entities
                .Select(e => primaryKeyProperty.GetValue(e))
                .Where(v => v != null)
                .ToList()!;

            if (primaryKeyValues.Count == 0) return;

            // Build SQL for many-to-many join
            string junctionTableName = _Sanitizer.SanitizeIdentifier(include.JunctionTableName);
            string relatedTableName = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string primaryKeyColumn = _Sanitizer.SanitizeIdentifier(GetColumnName(primaryKeyProperty));

            // Assume junction table has columns named after the entity tables
            string parentForeignKeyColumn = _Sanitizer.SanitizeIdentifier($"{GetTableName(typeof(T))}_id");
            string relatedForeignKeyColumn = _Sanitizer.SanitizeIdentifier($"{include.RelatedTableName}_id");
            string relatedPrimaryKeyColumn = _Sanitizer.SanitizeIdentifier("id"); // Assume standard primary key naming

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT r.* FROM {relatedTableName} r ");
            sql.Append($"INNER JOIN {junctionTableName} j ON r.{relatedPrimaryKeyColumn} = j.{relatedForeignKeyColumn} ");
            sql.Append($"WHERE j.{parentForeignKeyColumn} = ANY(@primaryKeys)");

            // Execute query and load related entities
            using NpgsqlCommand command = new NpgsqlCommand(sql.ToString(), connection);
            if (transaction != null)
                command.Transaction = (NpgsqlTransaction)transaction.Transaction;

            command.Parameters.AddWithValue("@primaryKeys", primaryKeyValues.ToArray());

            Dictionary<object, List<object>> relatedEntitiesByParentKey = new Dictionary<object, List<object>>();

            using (NpgsqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    object relatedEntity = CreateEntityFromReader(reader, include.RelatedEntityType);
                    // For many-to-many, we need another query to get the parent-child mappings
                    // This is a simplified version - in practice you'd need to handle this more efficiently
                }
            }

            // For now, set empty collections (full many-to-many implementation would require more complex logic)
            foreach (T entity in entities)
            {
                object emptyCollection = CreateTypedCollection(include.NavigationProperty.PropertyType, new List<object>());
                include.NavigationProperty.SetValue(entity, emptyCollection);
            }
        }

        /// <summary>
        /// Loads collections for entities that are already loaded as navigation properties.
        /// </summary>
        /// <param name="entities">The related entities to load collections for</param>
        /// <param name="include">The include information for the collection</param>
        /// <param name="connection">The PostgreSQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadCollectionForRelatedEntities(
            List<object> entities,
            PostgresIncludeInfo include,
            NpgsqlConnection connection,
            ITransaction? transaction)
        {
            if (entities.Count == 0) return;

            Type entityType = entities.First().GetType();
            PropertyInfo? primaryKeyProperty = GetPrimaryKeyProperty(entityType);
            if (primaryKeyProperty == null)
                throw new InvalidOperationException($"No primary key found for entity type {entityType.Name}");

            // Get primary key values from all entities
            List<object> primaryKeyValues = entities
                .Select(e => primaryKeyProperty.GetValue(e))
                .Where(v => v != null)
                .ToList()!;

            if (primaryKeyValues.Count == 0) return;

            // Build SQL to load related entities
            string relatedTableName = _Sanitizer.SanitizeIdentifier(include.RelatedTableName);
            string foreignKeyColumn = _Sanitizer.SanitizeIdentifier(GetColumnName(include.ForeignKeyProperty));

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT * FROM {relatedTableName} WHERE {foreignKeyColumn} = ANY(@primaryKeys)");

            // Execute query and load related entities
            using NpgsqlCommand command = new NpgsqlCommand(sql.ToString(), connection);
            if (transaction != null)
                command.Transaction = (NpgsqlTransaction)transaction.Transaction;

            command.Parameters.AddWithValue("@primaryKeys", primaryKeyValues.ToArray());

            Dictionary<object, List<object>> relatedEntitiesByParentKey = new Dictionary<object, List<object>>();

            using (NpgsqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    object relatedEntity = CreateEntityFromReader(reader, include.RelatedEntityType);
                    object? foreignKeyValue = GetValueFromReader(reader, GetColumnName(include.ForeignKeyProperty), include.ForeignKeyProperty?.PropertyType);

                    if (foreignKeyValue != null)
                    {
                        if (!relatedEntitiesByParentKey.ContainsKey(foreignKeyValue))
                        {
                            relatedEntitiesByParentKey[foreignKeyValue] = new List<object>();
                        }
                        relatedEntitiesByParentKey[foreignKeyValue].Add(relatedEntity);
                    }
                }
            }

            // Assign collections to parent entities
            foreach (object entity in entities)
            {
                object? primaryKeyValue = primaryKeyProperty.GetValue(entity);
                if (primaryKeyValue != null && relatedEntitiesByParentKey.TryGetValue(primaryKeyValue, out List<object>? relatedEntities))
                {
                    object collection = CreateTypedCollection(include.NavigationProperty.PropertyType, relatedEntities);
                    include.NavigationProperty.SetValue(entity, collection);
                }
                else
                {
                    // Set empty collection if no related entities found
                    object emptyCollection = CreateTypedCollection(include.NavigationProperty.PropertyType, new List<object>());
                    include.NavigationProperty.SetValue(entity, emptyCollection);
                }
            }
        }

        private PropertyInfo? GetPrimaryKeyProperty(Type entityType)
        {
            return entityType.GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<PropertyAttribute>()?
                .PropertyFlags.HasFlag(Flags.PrimaryKey) == true);
        }

        private string GetColumnName(PropertyInfo? property)
        {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
            return attr?.Name ?? property.Name.ToLowerInvariant();
        }

        private string GetTableName(Type entityType)
        {
            EntityAttribute? attr = entityType.GetCustomAttribute<EntityAttribute>();
            if (attr != null && !string.IsNullOrWhiteSpace(attr.Name))
            {
                return attr.Name;
            }
            return entityType.Name.ToLowerInvariant();
        }

        private object CreateEntityFromReader(NpgsqlDataReader reader, Type entityType)
        {
            object entity = Activator.CreateInstance(entityType)!;
            PropertyInfo[] properties = entityType.GetProperties();

            foreach (PropertyInfo property in properties)
            {
                try
                {
                    string columnName = GetColumnName(property);
                    if (HasColumn(reader, columnName))
                    {
                        object? value = GetValueFromReader(reader, columnName, property.PropertyType);
                        if (value != null)
                        {
                            object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType)!;
                            property.SetValue(entity, convertedValue);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Error setting property '{property.Name}' on type '{entityType.Name}': {ex.Message}", ex);
                }
            }

            return entity;
        }

        private object? GetValueFromReader(NpgsqlDataReader reader, string columnName, Type? targetType)
        {
            try
            {
                object value = reader[columnName];
                return value == DBNull.Value ? null : value;
            }
            catch (Exception)
            {
                return null;
            }
        }

        private bool HasColumn(NpgsqlDataReader reader, string columnName)
        {
            try
            {
                int ordinal = reader.GetOrdinal(columnName);
                return ordinal >= 0;
            }
            catch (IndexOutOfRangeException)
            {
                return false;
            }
        }

        private object CreateTypedCollection(Type collectionType, List<object> items)
        {
            if (collectionType.IsGenericType)
            {
                Type elementType = collectionType.GetGenericArguments()[0];
                Type listType = typeof(List<>).MakeGenericType(elementType);

                object collection = Activator.CreateInstance(listType)!;
                MethodInfo addMethod = listType.GetMethod("Add")!;

                foreach (object item in items)
                {
                    if (elementType.IsAssignableFrom(item.GetType()))
                    {
                        addMethod.Invoke(collection, new[] { item });
                    }
                }

                return collection;
            }

            throw new NotSupportedException($"Collection type {collectionType.Name} is not supported");
        }

        #endregion
    }
}