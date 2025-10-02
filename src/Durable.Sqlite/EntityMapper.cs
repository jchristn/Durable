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
    /// Maps SQLite query results to entity objects, handling joined tables and related entities.
    /// Supports complex object graphs with navigation properties.
    /// </summary>
    /// <typeparam name="T">The primary entity type being mapped</typeparam>
    internal class EntityMapper<T> where T : class, new()
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly IDataTypeConverter _DataTypeConverter;
        private readonly Dictionary<string, PropertyInfo> _BaseColumnMappings;
        private readonly Dictionary<string, HashSet<object>> _ProcessedEntities;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the EntityMapper class.
        /// </summary>
        /// <param name="dataTypeConverter">The data type converter for database values</param>
        /// <param name="baseColumnMappings">The column mappings for the primary entity</param>
        /// <exception cref="ArgumentNullException">Thrown when required parameters are null</exception>
        public EntityMapper(
            IDataTypeConverter dataTypeConverter,
            Dictionary<string, PropertyInfo> baseColumnMappings)
        {
            _DataTypeConverter = dataTypeConverter;
            _BaseColumnMappings = baseColumnMappings;
            _ProcessedEntities = new Dictionary<string, HashSet<object>>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Maps joined query results to a list of entities with related navigation properties populated.
        /// </summary>
        /// <param name="reader">The data reader containing the query results</param>
        /// <param name="joinResult">The join information from the query builder</param>
        /// <param name="includes">The list of navigation properties to include</param>
        /// <returns>A list of entities with related data populated</returns>
        /// <exception cref="ArgumentNullException">Thrown when required parameters are null</exception>
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

        #endregion

        #region Private-Methods

        private T MapPrimaryEntity(SqliteDataReader reader, Dictionary<object, T> existingEntities)
        {
            T entity = new T();

            foreach (KeyValuePair<string, PropertyInfo> kvp in _BaseColumnMappings)
            {
                string columnName = kvp.Key;
                PropertyInfo property = kvp.Value;

                try
                {
                    int ordinal = reader.GetOrdinal(columnName);
                    if (!reader.IsDBNull(ordinal))
                    {
                        object value = reader.GetValue(ordinal);
                        object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, property.PropertyType, property);
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
                        object convertedValue = _DataTypeConverter.ConvertFromDatabase(value, mapping.Property.PropertyType, mapping.Property);
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
            foreach (KeyValuePair<string, PropertyInfo> kvp in _BaseColumnMappings)
            {
                PropertyAttribute attr = kvp.Value.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey)
                {
                    return kvp.Value.GetValue(entity);
                }
            }
            return entity.GetHashCode();
        }

        #endregion
    }
}