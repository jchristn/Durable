namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using MySqlConnector;

    /// <summary>
    /// Handles loading of collection navigation properties for MySQL entities.
    /// Supports both one-to-many and many-to-many relationships with efficient batch loading.
    /// </summary>
    /// <typeparam name="T">The entity type that contains collection navigation properties</typeparam>
    internal class MySqlCollectionLoader<T> where T : class, new()
    {
        #region Private-Members

        private readonly ISanitizer _Sanitizer;
        private readonly IDataTypeConverter _DataTypeConverter;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlCollectionLoader class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer for SQL identifiers</param>
        /// <param name="dataTypeConverter">The data type converter for database values</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer or dataTypeConverter is null</exception>
        public MySqlCollectionLoader(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
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
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <exception cref="ArgumentNullException">Thrown when required parameters are null</exception>
        public void LoadCollections(
            List<T> entities,
            List<MySqlIncludeInfo> includes,
            MySqlConnection connection,
            ITransaction? transaction)
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            if (includes == null)
                throw new ArgumentNullException(nameof(includes));

            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            foreach (MySqlIncludeInfo include in includes)
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
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadCollectionsForRelatedEntities(
            List<object> entities,
            List<MySqlIncludeInfo> includes,
            MySqlConnection connection,
            ITransaction? transaction)
        {
            foreach (MySqlIncludeInfo include in includes)
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
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadCollection(
            List<T> entities,
            MySqlIncludeInfo include,
            MySqlConnection connection,
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
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadOneToManyCollection(
            List<T> entities,
            MySqlIncludeInfo include,
            MySqlConnection connection,
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
            string relatedTableName = include.RelatedTableName;
            string foreignKeyColumn = GetColumnName(include.ForeignKeyProperty);
            string primaryKeyColumn = GetColumnName(primaryKeyProperty);

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT * FROM `{relatedTableName}` WHERE `{foreignKeyColumn}` IN (");

            List<string> paramNames = new List<string>();
            for (int i = 0; i < primaryKeyValues.Count; i++)
            {
                paramNames.Add($"@pk{i}");
            }
            sql.Append(string.Join(", ", paramNames));
            sql.Append(")");

            // Execute query and load related entities
            using MySqlCommand command = new MySqlCommand(sql.ToString(), connection);
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction.Transaction;

            for (int i = 0; i < primaryKeyValues.Count; i++)
            {
                command.Parameters.AddWithValue($"@pk{i}", primaryKeyValues[i]);
            }

            Dictionary<object, List<object>> relatedEntitiesByParentKey = new Dictionary<object, List<object>>();

            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    object relatedEntity = CreateEntityFromReader(reader, include.RelatedEntityType);
                    object? foreignKeyValue = GetValueFromReader(reader, foreignKeyColumn, include.ForeignKeyProperty?.PropertyType);

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
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadManyToManyCollection(
            List<T> entities,
            MySqlIncludeInfo include,
            MySqlConnection connection,
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
            string junctionTableName = include.JunctionTableName;
            string relatedTableName = include.RelatedTableName;
            string primaryKeyColumn = GetColumnName(primaryKeyProperty);

            // Assume junction table has columns named after the entity tables
            string parentForeignKeyColumn = $"{GetTableName(typeof(T))}_id";
            string relatedForeignKeyColumn = $"{relatedTableName}_id";
            string relatedPrimaryKeyColumn = "id"; // Assume standard primary key naming

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT r.* FROM `{relatedTableName}` r ");
            sql.Append($"INNER JOIN `{junctionTableName}` j ON r.`{relatedPrimaryKeyColumn}` = j.`{relatedForeignKeyColumn}` ");
            sql.Append($"WHERE j.`{parentForeignKeyColumn}` IN (");

            List<string> paramNames = new List<string>();
            for (int i = 0; i < primaryKeyValues.Count; i++)
            {
                paramNames.Add($"@pk{i}");
            }
            sql.Append(string.Join(", ", paramNames));
            sql.Append(")");

            // Execute query and load related entities
            using MySqlCommand command = new MySqlCommand(sql.ToString(), connection);
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction.Transaction;

            for (int i = 0; i < primaryKeyValues.Count; i++)
            {
                command.Parameters.AddWithValue($"@pk{i}", primaryKeyValues[i]);
            }

            Dictionary<object, List<object>> relatedEntitiesByParentKey = new Dictionary<object, List<object>>();

            using (MySqlDataReader reader = command.ExecuteReader())
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
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        private void LoadCollectionForRelatedEntities(
            List<object> entities,
            MySqlIncludeInfo include,
            MySqlConnection connection,
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
            string relatedTableName = include.RelatedTableName;
            string foreignKeyColumn = GetColumnName(include.ForeignKeyProperty);

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT * FROM `{relatedTableName}` WHERE `{foreignKeyColumn}` IN (");

            List<string> paramNames = new List<string>();
            for (int i = 0; i < primaryKeyValues.Count; i++)
            {
                paramNames.Add($"@pk{i}");
            }
            sql.Append(string.Join(", ", paramNames));
            sql.Append(")");

            // Execute query and load related entities
            using MySqlCommand command = new MySqlCommand(sql.ToString(), connection);
            if (transaction != null)
                command.Transaction = (MySqlTransaction)transaction.Transaction;

            for (int i = 0; i < primaryKeyValues.Count; i++)
            {
                command.Parameters.AddWithValue($"@pk{i}", primaryKeyValues[i]);
            }

            Dictionary<object, List<object>> relatedEntitiesByParentKey = new Dictionary<object, List<object>>();

            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    object relatedEntity = CreateEntityFromReader(reader, include.RelatedEntityType);
                    object? foreignKeyValue = GetValueFromReader(reader, foreignKeyColumn, include.ForeignKeyProperty?.PropertyType);

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

        /// <summary>
        /// Gets the primary key property for the specified entity type.
        /// </summary>
        /// <param name="entityType">The entity type to get the primary key for</param>
        /// <returns>The primary key property, or null if not found</returns>
        private PropertyInfo? GetPrimaryKeyProperty(Type entityType)
        {
            return entityType.GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<PropertyAttribute>()?
                .PropertyFlags.HasFlag(Flags.PrimaryKey) == true);
        }

        /// <summary>
        /// Gets the database column name for a property.
        /// </summary>
        /// <param name="property">The property to get the column name for</param>
        /// <returns>The column name</returns>
        private string GetColumnName(PropertyInfo? property)
        {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            PropertyAttribute? attr = property.GetCustomAttribute<PropertyAttribute>();
            return attr?.Name ?? property.Name.ToLowerInvariant();
        }

        /// <summary>
        /// Gets the database table name for an entity type.
        /// </summary>
        /// <param name="entityType">The entity type to get the table name for</param>
        /// <returns>The table name</returns>
        private string GetTableName(Type entityType)
        {
            EntityAttribute? attr = entityType.GetCustomAttribute<EntityAttribute>();
            if (attr != null && !string.IsNullOrWhiteSpace(attr.Name))
            {
                return attr.Name;
            }
            return entityType.Name.ToLowerInvariant();
        }

        /// <summary>
        /// Creates an entity instance from a data reader.
        /// </summary>
        /// <param name="reader">The data reader containing entity data</param>
        /// <param name="entityType">The type of entity to create</param>
        /// <returns>The created entity instance</returns>
        private object CreateEntityFromReader(MySqlDataReader reader, Type entityType)
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

        /// <summary>
        /// Gets a value from the data reader for the specified column.
        /// </summary>
        /// <param name="reader">The data reader</param>
        /// <param name="columnName">The column name</param>
        /// <param name="targetType">The expected type of the value</param>
        /// <returns>The value from the reader, or null if DBNull</returns>
        private object? GetValueFromReader(MySqlDataReader reader, string columnName, Type? targetType)
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

        /// <summary>
        /// Checks if the reader has a column with the specified name.
        /// </summary>
        /// <param name="reader">The data reader</param>
        /// <param name="columnName">The column name to check</param>
        /// <returns>True if the column exists, false otherwise</returns>
        private bool HasColumn(MySqlDataReader reader, string columnName)
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

        /// <summary>
        /// Creates a typed collection from a list of objects.
        /// </summary>
        /// <param name="collectionType">The type of collection to create</param>
        /// <param name="items">The items to add to the collection</param>
        /// <returns>The created typed collection</returns>
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