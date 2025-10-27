#nullable enable

namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Helper class for building PostgreSQL schema (CREATE TABLE) SQL from entity metadata
    /// </summary>
    internal class PostgresSchemaBuilder
    {
        private readonly ISanitizer _sanitizer;
        private readonly IDataTypeConverter _dataTypeConverter;

        /// <summary>
        /// Initializes a new instance of the PostgresSchemaBuilder class
        /// </summary>
        /// <param name="sanitizer">The sanitizer for SQL identifiers</param>
        /// <param name="dataTypeConverter">The data type converter for SQL type mapping</param>
        public PostgresSchemaBuilder(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
        {
            _sanitizer = sanitizer ?? throw new ArgumentNullException(nameof(sanitizer));
            _dataTypeConverter = dataTypeConverter ?? throw new ArgumentNullException(nameof(dataTypeConverter));
        }

        /// <summary>
        /// Builds a CREATE TABLE statement for the specified entity type
        /// </summary>
        /// <param name="entityType">The entity type</param>
        /// <returns>The CREATE TABLE SQL statement</returns>
        public string BuildCreateTableSql(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            // Get entity metadata
            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type '{entityType.Name}' must have an Entity attribute");

            string tableName = entityAttr.Name;
            StringBuilder sql = new StringBuilder();
            List<string> columnDefinitions = new List<string>();
            List<string> foreignKeyConstraints = new List<string>();
            PropertyInfo? primaryKeyProperty = null;
            PropertyAttribute? primaryKeyAttr = null;

            // Process each property
            foreach (PropertyInfo property in entityType.GetProperties())
            {
                PropertyAttribute? propAttr = property.GetCustomAttribute<PropertyAttribute>();
                if (propAttr == null)
                    continue;

                // Build column definition
                string columnDef = BuildColumnDefinition(property, propAttr, out bool isPrimaryKey);
                columnDefinitions.Add(columnDef);

                if (isPrimaryKey)
                {
                    primaryKeyProperty = property;
                    primaryKeyAttr = propAttr;
                }

                // Check for foreign key
                ForeignKeyAttribute? fkAttr = property.GetCustomAttribute<ForeignKeyAttribute>();
                if (fkAttr != null)
                {
                    string fkConstraint = BuildForeignKeyConstraint(propAttr.Name, fkAttr);
                    foreignKeyConstraints.Add(fkConstraint);
                }
            }

            if (primaryKeyProperty == null)
                throw new InvalidOperationException($"Type '{entityType.Name}' must have a primary key column");

            // Build the full CREATE TABLE statement
            sql.Append("CREATE TABLE IF NOT EXISTS ");
            sql.Append(_sanitizer.SanitizeIdentifier(tableName));
            sql.Append(" (");
            sql.AppendLine();

            // Add column definitions
            for (int i = 0; i < columnDefinitions.Count; i++)
            {
                sql.Append("    ");
                sql.Append(columnDefinitions[i]);
                if (i < columnDefinitions.Count - 1 || foreignKeyConstraints.Count > 0)
                    sql.Append(",");
                sql.AppendLine();
            }

            // Add foreign key constraints
            for (int i = 0; i < foreignKeyConstraints.Count; i++)
            {
                sql.Append("    ");
                sql.Append(foreignKeyConstraints[i]);
                if (i < foreignKeyConstraints.Count - 1)
                    sql.Append(",");
                sql.AppendLine();
            }

            sql.Append(")");

            return sql.ToString();
        }

        /// <summary>
        /// Builds a column definition for a property
        /// </summary>
        private string BuildColumnDefinition(PropertyInfo property, PropertyAttribute propAttr, out bool isPrimaryKey)
        {
            StringBuilder columnDef = new StringBuilder();

            // Column name
            columnDef.Append(_sanitizer.SanitizeIdentifier(propAttr.Name));
            columnDef.Append(" ");

            // Check for auto-increment first
            bool hasAutoIncrement = (propAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement;
            isPrimaryKey = (propAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey;

            // Data type - PostgreSQL uses SERIAL/BIGSERIAL for auto-increment integers
            if (hasAutoIncrement && isPrimaryKey)
            {
                // Use SERIAL or BIGSERIAL for auto-incrementing primary keys
                Type underlyingType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                if (underlyingType == typeof(long) || underlyingType == typeof(ulong))
                {
                    columnDef.Append("BIGSERIAL");
                }
                else
                {
                    columnDef.Append("SERIAL");
                }
                // SERIAL already implies PRIMARY KEY in some contexts, but we'll add it explicitly below
            }
            else
            {
                string sqlType = _dataTypeConverter.GetDatabaseTypeString(property.PropertyType, property);
                columnDef.Append(sqlType);
            }

            // Primary key
            if (isPrimaryKey)
            {
                columnDef.Append(" PRIMARY KEY");
            }

            // Nullable (PostgreSQL defaults to nullable, so only specify NOT NULL if required)
            Type propertyType = property.PropertyType;
            bool isNullableType = propertyType.IsGenericType &&
                                  propertyType.GetGenericTypeDefinition() == typeof(Nullable<>);
            bool isReferenceType = !propertyType.IsValueType;

            // A column is NOT NULL if:
            // 1. It's a non-nullable value type (int, bool, DateTime, etc.) AND not explicitly marked as nullable
            // 2. For SERIAL/BIGSERIAL, NOT NULL is implied but we can add it explicitly
            // For safety, we'll mark value types as NOT NULL unless they're Nullable<T>
            if (!isNullableType && !isReferenceType && !isPrimaryKey && !hasAutoIncrement)
            {
                // For value types that aren't Nullable<T>, default to NOT NULL unless there's a default value
                DefaultValueAttribute? defaultAttr = property.GetCustomAttribute<DefaultValueAttribute>();
                if (defaultAttr == null)
                {
                    columnDef.Append(" NOT NULL");
                }
            }

            return columnDef.ToString();
        }

        /// <summary>
        /// Builds a foreign key constraint
        /// </summary>
        private string BuildForeignKeyConstraint(string columnName, ForeignKeyAttribute fkAttr)
        {
            StringBuilder fkDef = new StringBuilder();

            // Get referenced table name from the ReferencedType's EntityAttribute
            EntityAttribute? referencedEntityAttr = fkAttr.ReferencedType.GetCustomAttribute<EntityAttribute>();
            if (referencedEntityAttr == null)
                throw new InvalidOperationException($"Referenced type '{fkAttr.ReferencedType.Name}' must have an Entity attribute");

            string referencedTableName = referencedEntityAttr.Name;

            // Get referenced column name from the ReferencedProperty's PropertyAttribute
            PropertyInfo? referencedProp = fkAttr.ReferencedType.GetProperty(fkAttr.ReferencedProperty);
            if (referencedProp == null)
                throw new InvalidOperationException($"Referenced property '{fkAttr.ReferencedProperty}' not found on type '{fkAttr.ReferencedType.Name}'");

            PropertyAttribute? referencedPropAttr = referencedProp.GetCustomAttribute<PropertyAttribute>();
            if (referencedPropAttr == null)
                throw new InvalidOperationException($"Referenced property '{fkAttr.ReferencedProperty}' on type '{fkAttr.ReferencedType.Name}' must have a Property attribute");

            string referencedColumnName = referencedPropAttr.Name;

            // Build the foreign key constraint
            fkDef.Append("FOREIGN KEY (");
            fkDef.Append(_sanitizer.SanitizeIdentifier(columnName));
            fkDef.Append(") REFERENCES ");
            fkDef.Append(_sanitizer.SanitizeIdentifier(referencedTableName));
            fkDef.Append("(");
            fkDef.Append(_sanitizer.SanitizeIdentifier(referencedColumnName));
            fkDef.Append(")");

            // Note: OnDelete and OnUpdate are not currently supported by Durable's ForeignKeyAttribute
            // These could be added as optional properties if needed in the future

            return fkDef.ToString();
        }

        /// <summary>
        /// Gets the column information from the database for schema validation
        /// </summary>
        public static List<ColumnInfo> GetTableColumns(string tableName, string schemaName, System.Data.Common.DbConnection connection)
        {
            List<ColumnInfo> columns = new List<ColumnInfo>();

            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        CASE WHEN constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.table_schema, ku.table_name, ku.column_name, tc.constraint_type
                        FROM information_schema.key_column_usage ku
                        INNER JOIN information_schema.table_constraints tc
                            ON ku.constraint_name = tc.constraint_name
                            AND ku.table_schema = tc.table_schema
                            AND ku.table_name = tc.table_name
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON c.table_schema = pk.table_schema
                        AND c.table_name = pk.table_name
                        AND c.column_name = pk.column_name
                    WHERE c.table_schema = @schema AND c.table_name = @tableName
                    ORDER BY c.ordinal_position";

                System.Data.Common.DbParameter schemaParam = command.CreateParameter();
                schemaParam.ParameterName = "@schema";
                schemaParam.Value = schemaName;
                command.Parameters.Add(schemaParam);

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

                using (System.Data.Common.DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string dataType = reader.GetString(1);
                        string isNullable = reader.GetString(2);
                        object? defaultValue = reader.IsDBNull(3) ? null : reader.GetValue(3);
                        bool isPrimaryKey = reader.GetBoolean(4);

                        columns.Add(new ColumnInfo
                        {
                            Name = columnName,
                            Type = dataType,
                            NotNull = isNullable == "NO",
                            DefaultValue = defaultValue,
                            IsPrimaryKey = isPrimaryKey
                        });
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Checks if a table exists in the database
        /// </summary>
        public static bool TableExists(string tableName, string schemaName, System.Data.Common.DbConnection connection)
        {
            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema AND table_name = @tableName";

                System.Data.Common.DbParameter schemaParam = command.CreateParameter();
                schemaParam.ParameterName = "@schema";
                schemaParam.Value = schemaName;
                command.Parameters.Add(schemaParam);

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

                object? result = command.ExecuteScalar();
                return result != null && Convert.ToInt32(result) > 0;
            }
        }

        /// <summary>
        /// Builds CREATE INDEX SQL statements for the specified entity type
        /// </summary>
        public List<string> BuildCreateIndexSql(Type entityType)
        {
            if (entityType == null)
                throw new ArgumentNullException(nameof(entityType));

            List<string> indexSqlStatements = new List<string>();

            // Get entity metadata
            EntityAttribute? entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
            if (entityAttr == null)
                throw new InvalidOperationException($"Type '{entityType.Name}' must have an Entity attribute");

            string tableName = entityAttr.Name;

            // Build indexes from IndexAttribute on properties
            Dictionary<string, List<IndexPropertyInfo>> indexGroups =
                new Dictionary<string, List<IndexPropertyInfo>>();

            foreach (PropertyInfo property in entityType.GetProperties())
            {
                PropertyAttribute? propAttr = property.GetCustomAttribute<PropertyAttribute>();
                if (propAttr == null)
                    continue;

                IndexAttribute[] indexAttrs = property.GetCustomAttributes<IndexAttribute>().ToArray();
                foreach (IndexAttribute indexAttr in indexAttrs)
                {
                    string indexName = indexAttr.Name ?? $"idx_{tableName}_{propAttr.Name}";

                    if (!indexGroups.ContainsKey(indexName))
                    {
                        indexGroups[indexName] = new List<IndexPropertyInfo>();
                    }

                    indexGroups[indexName].Add(new IndexPropertyInfo { Property = property, IndexAttr = indexAttr });
                }
            }

            // Generate SQL for each index group
            foreach (KeyValuePair<string, List<IndexPropertyInfo>> group in indexGroups)
            {
                string indexName = group.Key;
                List<IndexPropertyInfo> columns = group.Value
                    .OrderBy(x => x.IndexAttr.Order)
                    .ToList();

                bool isUnique = columns.Any(x => x.IndexAttr.IsUnique);

                StringBuilder sql = new StringBuilder();
                sql.Append("CREATE ");
                if (isUnique)
                {
                    sql.Append("UNIQUE ");
                }
                sql.Append("INDEX IF NOT EXISTS ");
                sql.Append(_sanitizer.SanitizeIdentifier(indexName));
                sql.Append(" ON ");
                sql.Append(_sanitizer.SanitizeIdentifier(tableName));
                sql.Append(" (");

                for (int i = 0; i < columns.Count; i++)
                {
                    PropertyAttribute? propAttr = columns[i].Property.GetCustomAttribute<PropertyAttribute>();
                    if (propAttr != null)
                    {
                        sql.Append(_sanitizer.SanitizeIdentifier(propAttr.Name));
                        if (i < columns.Count - 1)
                        {
                            sql.Append(", ");
                        }
                    }
                }

                sql.Append(")");

                indexSqlStatements.Add(sql.ToString());
            }

            // Build indexes from CompositeIndexAttribute on class
            CompositeIndexAttribute[] compositeIndexAttrs = entityType.GetCustomAttributes<CompositeIndexAttribute>().ToArray();
            foreach (CompositeIndexAttribute compositeAttr in compositeIndexAttrs)
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("CREATE ");
                if (compositeAttr.IsUnique)
                {
                    sql.Append("UNIQUE ");
                }
                sql.Append("INDEX IF NOT EXISTS ");
                sql.Append(_sanitizer.SanitizeIdentifier(compositeAttr.Name));
                sql.Append(" ON ");
                sql.Append(_sanitizer.SanitizeIdentifier(tableName));
                sql.Append(" (");

                for (int i = 0; i < compositeAttr.ColumnNames.Length; i++)
                {
                    sql.Append(_sanitizer.SanitizeIdentifier(compositeAttr.ColumnNames[i]));
                    if (i < compositeAttr.ColumnNames.Length - 1)
                    {
                        sql.Append(", ");
                    }
                }

                sql.Append(")");

                indexSqlStatements.Add(sql.ToString());
            }

            return indexSqlStatements;
        }

        /// <summary>
        /// Gets existing indexes for a table from the database
        /// </summary>
        public static List<IndexInfo> GetExistingIndexes(string tableName, string schemaName, System.Data.Common.DbConnection connection)
        {
            List<IndexInfo> indexes = new List<IndexInfo>();

            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT
                        i.indexname,
                        i.indexdef
                    FROM pg_indexes i
                    WHERE i.schemaname = @schema
                        AND i.tablename = @tableName
                    ORDER BY i.indexname";

                System.Data.Common.DbParameter schemaParam = command.CreateParameter();
                schemaParam.ParameterName = "@schema";
                schemaParam.Value = schemaName;
                command.Parameters.Add(schemaParam);

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

                using (System.Data.Common.DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        string definition = reader.GetString(1);
                        bool isUnique = definition.ToUpper().Contains("UNIQUE");

                        // Skip primary key indexes
                        if (name.EndsWith("_pkey"))
                            continue;

                        indexes.Add(new IndexInfo
                        {
                            Name = name,
                            TableName = tableName,
                            IsUnique = isUnique
                        });
                    }
                }
            }

            return indexes;
        }

        /// <summary>
        /// Checks if an index exists in the database
        /// </summary>
        public static bool IndexExists(string indexName, string tableName, string schemaName, System.Data.Common.DbConnection connection)
        {
            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT COUNT(*)
                    FROM pg_indexes
                    WHERE schemaname = @schema
                        AND tablename = @tableName
                        AND indexname = @indexName";

                System.Data.Common.DbParameter schemaParam = command.CreateParameter();
                schemaParam.ParameterName = "@schema";
                schemaParam.Value = schemaName;
                command.Parameters.Add(schemaParam);

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

                System.Data.Common.DbParameter nameParam = command.CreateParameter();
                nameParam.ParameterName = "@indexName";
                nameParam.Value = indexName;
                command.Parameters.Add(nameParam);

                object? result = command.ExecuteScalar();
                return result != null && Convert.ToInt32(result) > 0;
            }
        }

        /// <summary>
        /// Checks if a database exists
        /// </summary>
        public static bool DatabaseExists(string databaseName, System.Data.Common.DbConnection connection)
        {
            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT COUNT(*) FROM pg_database WHERE datname = @database";

                System.Data.Common.DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = "@database";
                dbParam.Value = databaseName;
                command.Parameters.Add(dbParam);

                object? result = command.ExecuteScalar();
                return result != null && Convert.ToInt32(result) > 0;
            }
        }
    }

    /// <summary>
    /// Represents a property with its associated index attribute
    /// </summary>
    internal class IndexPropertyInfo
    {
        public PropertyInfo Property { get; set; } = null!;
        public IndexAttribute IndexAttr { get; set; } = null!;
    }

    /// <summary>
    /// Represents column information from the database
    /// </summary>
    internal class ColumnInfo
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public bool NotNull { get; set; }
        public object? DefaultValue { get; set; }
        public bool IsPrimaryKey { get; set; }
    }

    /// <summary>
    /// Represents index information from the database
    /// </summary>
    internal class IndexInfo
    {
        public string Name { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public List<string> Columns { get; set; } = new List<string>();
        public bool IsUnique { get; set; }
    }
}
