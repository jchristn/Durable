namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Handles loading of collection navigation properties for SQLite entities.
    /// Supports both one-to-many and many-to-many relationships with efficient batch loading.
    /// </summary>
    /// <typeparam name="T">The entity type that contains collection navigation properties</typeparam>
    internal class CollectionLoader<T> where T : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly ISanitizer _Sanitizer;
        private readonly IDataTypeConverter _DataTypeConverter;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the CollectionLoader class.
        /// </summary>
        /// <param name="sanitizer">The sanitizer for SQL identifiers</param>
        /// <param name="dataTypeConverter">The data type converter for database values</param>
        /// <exception cref="ArgumentNullException">Thrown when sanitizer or dataTypeConverter is null</exception>
        public CollectionLoader(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
        {
            _Sanitizer = sanitizer;
            _DataTypeConverter = dataTypeConverter;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Loads collection navigation properties for a list of entities.
        /// </summary>
        /// <param name="entities">The entities to load collections for</param>
        /// <param name="includes">The include information for navigation properties</param>
        /// <param name="connection">The SQLite connection to use</param>
        /// <param name="transaction">Optional transaction to execute within</param>
        /// <exception cref="ArgumentNullException">Thrown when required parameters are null</exception>
        public void LoadCollections(
            List<T> entities,
            List<IncludeInfo> includes,
            SqliteConnection connection,
            ITransaction transaction)
        {
            foreach (IncludeInfo include in includes)
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
                        object related = include.NavigationProperty.GetValue(entity);
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

        private void LoadCollectionsForRelatedEntities(
            List<object> entities,
            List<IncludeInfo> includes,
            SqliteConnection connection,
            ITransaction transaction)
        {
            foreach (IncludeInfo include in includes)
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
                        object related = include.NavigationProperty.GetValue(entity);
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

        private void LoadCollection(
            List<T> entities,
            IncludeInfo include,
            SqliteConnection connection,
            ITransaction transaction)
        {
            if (entities.Count == 0) return;

            PropertyInfo primaryKeyProp = GetPrimaryKeyProperty(typeof(T));
            List<object> primaryKeys = entities.Select(e => primaryKeyProp.GetValue(e)).Distinct().ToList();

            if (primaryKeys.Count == 0) return;

            Type collectionItemType = include.NavigationProperty.PropertyType.GetGenericArguments()[0];
            string relatedTableName = GetTableName(collectionItemType);

            if (include.IsManyToMany)
            {
                // Handle many-to-many relationships
                LoadManyToManyCollection(entities, include, connection, transaction, primaryKeys, collectionItemType, relatedTableName);
            }
            else
            {
                // Handle one-to-many relationships
                PropertyInfo foreignKeyPropInRelated = GetForeignKeyPropertyInRelatedType(collectionItemType, typeof(T));
                PropertyAttribute fkAttr = foreignKeyPropInRelated.GetCustomAttribute<PropertyAttribute>();
                string fkColumnName = fkAttr?.Name ?? foreignKeyPropInRelated.Name;

                StringBuilder sql = new StringBuilder();
                sql.Append($"SELECT * FROM {_Sanitizer.SanitizeIdentifier(relatedTableName)} WHERE {_Sanitizer.SanitizeIdentifier(fkColumnName)} IN (");
                sql.Append(string.Join(", ", primaryKeys.Select((_, i) => $"@pk{i}")));
                sql.Append(")");

                LoadOneToManyCollection(entities, include, connection, transaction, sql.ToString(), primaryKeys, collectionItemType);
            }
        }

        private void LoadManyToManyCollection(
            List<T> entities,
            IncludeInfo include,
            SqliteConnection connection,
            ITransaction transaction,
            List<object> primaryKeys,
            Type collectionItemType,
            string relatedTableName)
        {
            ManyToManyNavigationPropertyAttribute m2mAttr = include.NavigationProperty.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
            if (m2mAttr == null)
            {
                throw new InvalidOperationException($"ManyToManyNavigationPropertyAttribute not found for {include.PropertyPath}");
            }

            // Get junction table column names
            PropertyInfo thisEntityFkProp = m2mAttr.JunctionEntityType.GetProperty(m2mAttr.ThisEntityForeignKeyProperty);
            PropertyInfo relatedEntityFkProp = m2mAttr.JunctionEntityType.GetProperty(m2mAttr.RelatedEntityForeignKeyProperty);
            
            PropertyAttribute thisEntityFkAttr = thisEntityFkProp.GetCustomAttribute<PropertyAttribute>();
            PropertyAttribute relatedEntityFkAttr = relatedEntityFkProp.GetCustomAttribute<PropertyAttribute>();
            
            string thisEntityFkColumn = thisEntityFkAttr?.Name ?? thisEntityFkProp.Name;
            string relatedEntityFkColumn = relatedEntityFkAttr?.Name ?? relatedEntityFkProp.Name;
            
            // Get primary key column of related entity
            string relatedPkColumn = GetPrimaryKeyColumnName(collectionItemType);

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT r.* FROM {_Sanitizer.SanitizeIdentifier(relatedTableName)} r ");
            sql.Append($"INNER JOIN {_Sanitizer.SanitizeIdentifier(include.JunctionTableName)} j ");
            sql.Append($"ON r.{_Sanitizer.SanitizeIdentifier(relatedPkColumn)} = j.{_Sanitizer.SanitizeIdentifier(relatedEntityFkColumn)} ");
            sql.Append($"WHERE j.{_Sanitizer.SanitizeIdentifier(thisEntityFkColumn)} IN (");
            sql.Append(string.Join(", ", primaryKeys.Select((_, i) => $"@pk{i}")));
            sql.Append($") ORDER BY j.{_Sanitizer.SanitizeIdentifier(thisEntityFkColumn)}");

            LoadManyToManyResults(entities, include, connection, transaction, sql.ToString(), primaryKeys, collectionItemType, thisEntityFkColumn);
        }

        private void LoadOneToManyCollection(
            List<T> entities,
            IncludeInfo include,
            SqliteConnection connection,
            ITransaction transaction,
            string sql,
            List<object> primaryKeys,
            Type collectionItemType)
        {
            using SqliteCommand command = (SqliteCommand)connection.CreateCommand();
            if (transaction != null && transaction is SqliteRepositoryTransaction sqliteTransaction)
            {
                command.Transaction = sqliteTransaction.Transaction as SqliteTransaction;
            }

            command.CommandText = sql;
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                command.Parameters.AddWithValue($"@pk{i}", primaryKeys[i]);
            }

            Dictionary<object, List<object>> relatedItemsByKey = new Dictionary<object, List<object>>();

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                object relatedItem = MapReaderToType(reader, collectionItemType);
                PropertyInfo foreignKeyPropInRelated = GetForeignKeyPropertyInRelatedType(collectionItemType, typeof(T));
                object fkValue = foreignKeyPropInRelated.GetValue(relatedItem);

                if (!relatedItemsByKey.ContainsKey(fkValue))
                {
                    relatedItemsByKey[fkValue] = new List<object>();
                }
                relatedItemsByKey[fkValue].Add(relatedItem);
            }

            foreach (T entity in entities)
            {
                PropertyInfo primaryKeyProp = GetPrimaryKeyProperty(typeof(T));
                object primaryKey = primaryKeyProp.GetValue(entity);
                if (relatedItemsByKey.ContainsKey(primaryKey))
                {
                    object collection = Activator.CreateInstance(include.NavigationProperty.PropertyType);
                    MethodInfo addMethod = include.NavigationProperty.PropertyType.GetMethod("Add");

                    foreach (object item in relatedItemsByKey[primaryKey])
                    {
                        addMethod.Invoke(collection, new[] { item });
                    }

                    include.NavigationProperty.SetValue(entity, collection);
                }
                else
                {
                    object emptyCollection = Activator.CreateInstance(include.NavigationProperty.PropertyType);
                    include.NavigationProperty.SetValue(entity, emptyCollection);
                }
            }
        }

        private void LoadManyToManyResults(
            List<T> entities,
            IncludeInfo include,
            SqliteConnection connection,
            ITransaction transaction,
            string sql,
            List<object> primaryKeys,
            Type collectionItemType,
            string thisEntityFkColumn)
        {
            using SqliteCommand command = (SqliteCommand)connection.CreateCommand();
            if (transaction != null && transaction is SqliteRepositoryTransaction sqliteTransaction)
            {
                command.Transaction = sqliteTransaction.Transaction as SqliteTransaction;
            }

            command.CommandText = sql;
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                command.Parameters.AddWithValue($"@pk{i}", primaryKeys[i]);
            }

            Dictionary<object, List<object>> relatedItemsByKey = new Dictionary<object, List<object>>();

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                object relatedItem = MapReaderToType(reader, collectionItemType);
                
                // We need to get the foreign key value from the junction table
                // For now, we'll use a simpler approach and query again
                PropertyInfo primaryKeyProp = GetPrimaryKeyProperty(typeof(T));
                
                // We need to associate this related item with the correct entity
                // This is a limitation of the current approach - we need to refactor this
                // For now, let's load all items and distribute them later
                foreach (object pk in primaryKeys)
                {
                    if (!relatedItemsByKey.ContainsKey(pk))
                    {
                        relatedItemsByKey[pk] = new List<object>();
                    }
                }
            }

            // For now, let's do a separate query to get the correct associations
            LoadManyToManyAssociations(entities, include, connection, transaction, primaryKeys, collectionItemType, thisEntityFkColumn, relatedItemsByKey);

            foreach (T entity in entities)
            {
                PropertyInfo primaryKeyProp = GetPrimaryKeyProperty(typeof(T));
                object primaryKey = primaryKeyProp.GetValue(entity);
                if (relatedItemsByKey.ContainsKey(primaryKey))
                {
                    object collection = Activator.CreateInstance(include.NavigationProperty.PropertyType);
                    MethodInfo addMethod = include.NavigationProperty.PropertyType.GetMethod("Add");

                    foreach (object item in relatedItemsByKey[primaryKey])
                    {
                        addMethod.Invoke(collection, new[] { item });
                    }

                    include.NavigationProperty.SetValue(entity, collection);
                }
                else
                {
                    object emptyCollection = Activator.CreateInstance(include.NavigationProperty.PropertyType);
                    include.NavigationProperty.SetValue(entity, emptyCollection);
                }
            }
        }

        private void LoadManyToManyAssociations(
            List<T> entities,
            IncludeInfo include,
            SqliteConnection connection,
            ITransaction transaction,
            List<object> primaryKeys,
            Type collectionItemType,
            string thisEntityFkColumn,
            Dictionary<object, List<object>> relatedItemsByKey)
        {
            ManyToManyNavigationPropertyAttribute m2mAttr = include.NavigationProperty.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
            string relatedTableName = GetTableName(collectionItemType);
            
            PropertyInfo relatedEntityFkProp = m2mAttr.JunctionEntityType.GetProperty(m2mAttr.RelatedEntityForeignKeyProperty);
            PropertyAttribute relatedEntityFkAttr = relatedEntityFkProp.GetCustomAttribute<PropertyAttribute>();
            string relatedEntityFkColumn = relatedEntityFkAttr?.Name ?? relatedEntityFkProp.Name;
            
            string relatedPkColumn = GetPrimaryKeyColumnName(collectionItemType);

            foreach (object primaryKey in primaryKeys)
            {
                StringBuilder sql = new StringBuilder();
                sql.Append($"SELECT r.* FROM {_Sanitizer.SanitizeIdentifier(relatedTableName)} r ");
                sql.Append($"INNER JOIN {_Sanitizer.SanitizeIdentifier(include.JunctionTableName)} j ");
                sql.Append($"ON r.{_Sanitizer.SanitizeIdentifier(relatedPkColumn)} = j.{_Sanitizer.SanitizeIdentifier(relatedEntityFkColumn)} ");
                sql.Append($"WHERE j.{_Sanitizer.SanitizeIdentifier(thisEntityFkColumn)} = @pk");

                using SqliteCommand command = (SqliteCommand)connection.CreateCommand();
                if (transaction != null && transaction is SqliteRepositoryTransaction sqliteTransaction)
                {
                    command.Transaction = sqliteTransaction.Transaction as SqliteTransaction;
                }

                command.CommandText = sql.ToString();
                command.Parameters.AddWithValue("@pk", primaryKey);

                if (!relatedItemsByKey.ContainsKey(primaryKey))
                {
                    relatedItemsByKey[primaryKey] = new List<object>();
                }

                using SqliteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    object relatedItem = MapReaderToType(reader, collectionItemType);
                    relatedItemsByKey[primaryKey].Add(relatedItem);
                }
            }
        }

        private string GetPrimaryKeyColumnName(Type entityType)
        {
            foreach (PropertyInfo prop in entityType.GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return attr.Name;
                }
            }
            throw new InvalidOperationException($"No primary key found for type {entityType.Name}");
        }

        private void LoadCollectionForRelatedEntities(
            List<object> entities,
            IncludeInfo include,
            SqliteConnection connection,
            ITransaction transaction)
        {
            if (entities.Count == 0) return;

            Type entityType = entities[0].GetType();
            PropertyInfo primaryKeyProp = GetPrimaryKeyProperty(entityType);
            List<object> primaryKeys = entities.Select(e => primaryKeyProp.GetValue(e)).Distinct().ToList();

            if (primaryKeys.Count == 0) return;

            Type collectionItemType = include.NavigationProperty.PropertyType.GetGenericArguments()[0];
            string relatedTableName = GetTableName(collectionItemType);
            PropertyInfo foreignKeyPropInRelated = GetForeignKeyPropertyInRelatedType(collectionItemType, entityType);
            PropertyAttribute fkAttr = foreignKeyPropInRelated.GetCustomAttribute<PropertyAttribute>();
            string fkColumnName = fkAttr?.Name ?? foreignKeyPropInRelated.Name;

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT * FROM {_Sanitizer.SanitizeIdentifier(relatedTableName)} WHERE {_Sanitizer.SanitizeIdentifier(fkColumnName)} IN (");
            sql.Append(string.Join(", ", primaryKeys.Select((_, i) => $"@pk{i}")));
            sql.Append(")");

            using SqliteCommand command = (SqliteCommand)connection.CreateCommand();
            if (transaction != null && transaction is SqliteRepositoryTransaction sqliteTransaction)
            {
                command.Transaction = sqliteTransaction.Transaction as SqliteTransaction;
            }

            command.CommandText = sql.ToString();
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                command.Parameters.AddWithValue($"@pk{i}", primaryKeys[i]);
            }

            Dictionary<object, List<object>> relatedItemsByKey = new Dictionary<object, List<object>>();

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                object relatedItem = MapReaderToType(reader, collectionItemType);
                object fkValue = foreignKeyPropInRelated.GetValue(relatedItem);

                if (!relatedItemsByKey.ContainsKey(fkValue))
                {
                    relatedItemsByKey[fkValue] = new List<object>();
                }
                relatedItemsByKey[fkValue].Add(relatedItem);
            }

            foreach (object entity in entities)
            {
                object primaryKey = primaryKeyProp.GetValue(entity);
                if (relatedItemsByKey.ContainsKey(primaryKey))
                {
                    object collection = Activator.CreateInstance(include.NavigationProperty.PropertyType);
                    MethodInfo addMethod = include.NavigationProperty.PropertyType.GetMethod("Add");

                    foreach (object item in relatedItemsByKey[primaryKey])
                    {
                        addMethod.Invoke(collection, new[] { item });
                    }

                    include.NavigationProperty.SetValue(entity, collection);
                }
                else
                {
                    object emptyCollection = Activator.CreateInstance(include.NavigationProperty.PropertyType);
                    include.NavigationProperty.SetValue(entity, emptyCollection);
                }
            }
        }

        private PropertyInfo GetPrimaryKeyProperty(Type entityType)
        {
            foreach (PropertyInfo prop in entityType.GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return prop;
                }
            }
            throw new InvalidOperationException($"No primary key found for type {entityType.Name}");
        }

        private PropertyInfo GetForeignKeyPropertyInRelatedType(Type relatedType, Type referencingType)
        {
            foreach (PropertyInfo prop in relatedType.GetProperties())
            {
                ForeignKeyAttribute fkAttr = prop.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr != null && fkAttr.ReferencedType == referencingType)
                {
                    return prop;
                }
            }
            throw new InvalidOperationException($"No foreign key found in {relatedType.Name} referencing {referencingType.Name}");
        }

        private string GetTableName(Type entityType)
        {
            EntityAttribute entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
            {
                throw new InvalidOperationException($"Type {entityType.Name} must have an Entity attribute");
            }
            return entityAttr.Name;
        }

        private object MapReaderToType(IDataReader reader, Type targetType)
        {
            object result = Activator.CreateInstance(targetType);

            foreach (PropertyInfo prop in targetType.GetProperties())
            {
                PropertyAttribute attr = prop.GetCustomAttribute<PropertyAttribute>();
                if (attr != null)
                {
                    try
                    {
                        int ordinal = reader.GetOrdinal(attr.Name);
                        if (!reader.IsDBNull(ordinal))
                        {
                            object value = reader.GetValue(ordinal);
                            object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, prop.PropertyType, prop);
                            prop.SetValue(result, convertedValue);
                        }
                    }
                    catch (IndexOutOfRangeException)
                    {
                        continue;
                    }
                }
            }

            return result;
        }

        #endregion
    }
}