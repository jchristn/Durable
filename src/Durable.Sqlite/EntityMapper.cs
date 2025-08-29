namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    internal class EntityMapper<T> where T : class, new()
    {
        private readonly IDataTypeConverter _dataTypeConverter;
        private readonly Dictionary<string, PropertyInfo> _baseColumnMappings;
        private readonly Dictionary<string, HashSet<object>> _processedEntities;

        public EntityMapper(
            IDataTypeConverter dataTypeConverter,
            Dictionary<string, PropertyInfo> baseColumnMappings)
        {
            _dataTypeConverter = dataTypeConverter;
            _baseColumnMappings = baseColumnMappings;
            _processedEntities = new Dictionary<string, HashSet<object>>();
        }

        public List<T> MapJoinedResults(
            SqliteDataReader reader,
            JoinBuilder.JoinResult joinResult,
            List<IncludeInfo> includes)
        {
            Dictionary<object, T> primaryEntities = new Dictionary<object, T>();
            List<T> results = new List<T>();

            while (reader.Read())
            {
                T primaryEntity = MapPrimaryEntity(reader, primaryEntities);

                if (!primaryEntities.ContainsKey(GetEntityKey(primaryEntity)))
                {
                    primaryEntities[GetEntityKey(primaryEntity)] = primaryEntity;
                    results.Add(primaryEntity);
                }
                else
                {
                    primaryEntity = primaryEntities[GetEntityKey(primaryEntity)];
                }

                MapRelatedEntities(reader, primaryEntity, includes, joinResult);
            }

            return results;
        }

        private T MapPrimaryEntity(SqliteDataReader reader, Dictionary<object, T> existingEntities)
        {
            T entity = new T();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _baseColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                try
                {
                    int ordinal = reader.GetOrdinal(columnName);
                    if (!reader.IsDBNull(ordinal))
                    {
                        object value = reader.GetValue(ordinal);
                        object convertedValue = _dataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property);
                        property.SetValue(entity, convertedValue);
                    }
                }
                catch (IndexOutOfRangeException)
                {
                    continue;
                }
            }

            return entity;
        }

        private void MapRelatedEntities(
            SqliteDataReader reader,
            T primaryEntity,
            List<IncludeInfo> includes,
            JoinBuilder.JoinResult joinResult)
        {
            foreach (IncludeInfo include in includes)
            {
                if (include.IsCollection)
                {
                    continue;
                }

                object relatedEntity = MapRelatedEntity(reader, include, joinResult);

                if (relatedEntity != null)
                {
                    include.NavigationProperty.SetValue(primaryEntity, relatedEntity);

                    if (include.Children.Count > 0)
                    {
                        MapNestedRelatedEntities(reader, relatedEntity, include.Children, joinResult);
                    }
                }
            }
        }

        private void MapNestedRelatedEntities(
            SqliteDataReader reader,
            object parentEntity,
            List<IncludeInfo> includes,
            JoinBuilder.JoinResult joinResult)
        {
            foreach (IncludeInfo include in includes)
            {
                if (include.IsCollection)
                {
                    continue;
                }

                object relatedEntity = MapRelatedEntity(reader, include, joinResult);

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

        private object MapRelatedEntity(
            SqliteDataReader reader,
            IncludeInfo include,
            JoinBuilder.JoinResult joinResult)
        {
            if (!joinResult.ColumnMappingsByAlias.ContainsKey(include.JoinAlias))
            {
                return null;
            }

            List<JoinBuilder.ColumnMapping> mappings = joinResult.ColumnMappingsByAlias[include.JoinAlias];
            object entity = Activator.CreateInstance(include.RelatedEntityType);

            bool hasAnyValue = false;

            foreach (JoinBuilder.ColumnMapping mapping in mappings)
            {
                try
                {
                    string columnName = mapping.Alias ?? mapping.ColumnName;
                    int ordinal = reader.GetOrdinal(columnName);

                    if (!reader.IsDBNull(ordinal))
                    {
                        hasAnyValue = true;
                        object value = reader.GetValue(ordinal);
                        object convertedValue = _dataTypeConverter.ConvertFromDatabase(value, mapping.Property.PropertyType, mapping.Property);
                        mapping.Property.SetValue(entity, convertedValue);
                    }
                }
                catch (IndexOutOfRangeException)
                {
                    continue;
                }
            }

            return hasAnyValue ? entity : null;
        }

        private object GetEntityKey(T entity)
        {
            foreach (KeyValuePair<string, PropertyInfo> kvp in _baseColumnMappings)
            {
                PropertyAttribute attr = kvp.Value.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return kvp.Value.GetValue(entity);
                }
            }
            return entity.GetHashCode();
        }
    }

    internal class CollectionLoader<T> where T : class, new()
    {
        private readonly ISanitizer _sanitizer;
        private readonly IDataTypeConverter _dataTypeConverter;

        public CollectionLoader(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
        {
            _sanitizer = sanitizer;
            _dataTypeConverter = dataTypeConverter;
        }

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
            PropertyInfo foreignKeyPropInRelated = GetForeignKeyPropertyInRelatedType(collectionItemType, typeof(T));
            PropertyAttribute fkAttr = foreignKeyPropInRelated.GetCustomAttribute<PropertyAttribute>();
            string fkColumnName = fkAttr?.Name ?? foreignKeyPropInRelated.Name;

            StringBuilder sql = new StringBuilder();
            sql.Append($"SELECT * FROM {_sanitizer.SanitizeIdentifier(relatedTableName)} WHERE {_sanitizer.SanitizeIdentifier(fkColumnName)} IN (");
            sql.Append(string.Join(", ", primaryKeys.Select((_, i) => $"@pk{i}")));
            sql.Append(")");

            using SqliteCommand command = connection.CreateCommand();
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

            foreach (T entity in entities)
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
            sql.Append($"SELECT * FROM {_sanitizer.SanitizeIdentifier(relatedTableName)} WHERE {_sanitizer.SanitizeIdentifier(fkColumnName)} IN (");
            sql.Append(string.Join(", ", primaryKeys.Select((_, i) => $"@pk{i}")));
            sql.Append(")");

            using SqliteCommand command = connection.CreateCommand();
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
                            object convertedValue = _dataTypeConverter.ConvertFromDatabase(value, prop.PropertyType, prop);
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
    }
}