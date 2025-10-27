#nullable enable

namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Helper class for building MySQL schema (CREATE TABLE) SQL from entity metadata
    /// </summary>
    internal class MySqlSchemaBuilder
    {
        private readonly ISanitizer _sanitizer;
        private readonly IDataTypeConverter _dataTypeConverter;

        /// <summary>
        /// Initializes a new instance of the MySqlSchemaBuilder class
        /// </summary>
        /// <param name="sanitizer">The sanitizer for SQL identifiers</param>
        /// <param name="dataTypeConverter">The data type converter for SQL type mapping</param>
        public MySqlSchemaBuilder(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
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

            sql.Append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

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

            // Data type
            string sqlType = _dataTypeConverter.GetDatabaseTypeString(property.PropertyType, property);
            columnDef.Append(sqlType);

            // Primary key
            isPrimaryKey = (propAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey;

            // Auto-increment (must come before PRIMARY KEY in MySQL)
            if ((propAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement)
            {
                columnDef.Append(" AUTO_INCREMENT");
            }

            if (isPrimaryKey)
            {
                columnDef.Append(" PRIMARY KEY");
            }

            // Nullable (MySQL defaults to nullable, so only specify NOT NULL if required)
            Type propertyType = property.PropertyType;
            bool isNullableType = propertyType.IsGenericType &&
                                  propertyType.GetGenericTypeDefinition() == typeof(Nullable<>);
            bool isReferenceType = !propertyType.IsValueType;

            // A column is NOT NULL if:
            // 1. It's a non-nullable value type (int, bool, DateTime, etc.) AND not explicitly marked as nullable
            // 2. It's a reference type but the property is not nullable-annotated (hard to detect at runtime)
            // For safety, we'll mark value types as NOT NULL unless they're Nullable<T>
            if (!isNullableType && !isReferenceType && !isPrimaryKey)
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
        public static List<ColumnInfo> GetTableColumns(string tableName, System.Data.Common.DbConnection connection)
        {
            List<ColumnInfo> columns = new List<ColumnInfo>();

            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = $"SHOW COLUMNS FROM `{tableName}`";
                using (System.Data.Common.DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string field = reader.GetString(0);  // Field name
                        string type = reader.GetString(1);   // Type
                        string nullable = reader.GetString(2); // Null (YES/NO)
                        string key = reader.GetString(3);    // Key (PRI, UNI, MUL)
                        object? defaultValue = reader.IsDBNull(4) ? null : reader.GetValue(4);

                        columns.Add(new ColumnInfo
                        {
                            Name = field,
                            Type = type,
                            NotNull = nullable == "NO",
                            DefaultValue = defaultValue,
                            IsPrimaryKey = key == "PRI"
                        });
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Checks if a table exists in the database
        /// </summary>
        public static bool TableExists(string tableName, string? databaseName, System.Data.Common.DbConnection connection, System.Data.Common.DbTransaction? transaction = null)
        {
            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                // If database name is not provided, use the connection's database
                string dbToCheck = databaseName;
                if (string.IsNullOrWhiteSpace(dbToCheck))
                {
                    dbToCheck = connection.Database;
                }

                command.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @database AND table_name = @tableName";

                System.Data.Common.DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = "@database";
                dbParam.Value = dbToCheck;
                command.Parameters.Add(dbParam);

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
                sql.Append("INDEX ");
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
                sql.Append("INDEX ");
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
        public static List<IndexInfo> GetExistingIndexes(string tableName, string databaseName, System.Data.Common.DbConnection connection)
        {
            List<IndexInfo> indexes = new List<IndexInfo>();

            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT
                        INDEX_NAME,
                        NON_UNIQUE
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = @database
                        AND TABLE_NAME = @tableName
                        AND INDEX_NAME != 'PRIMARY'
                    GROUP BY INDEX_NAME, NON_UNIQUE
                    ORDER BY INDEX_NAME";

                System.Data.Common.DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = "@database";
                dbParam.Value = databaseName;
                command.Parameters.Add(dbParam);

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

                using (System.Data.Common.DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        int nonUnique = reader.GetInt32(1);
                        bool isUnique = nonUnique == 0;

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
        public static bool IndexExists(string indexName, string tableName, string databaseName, System.Data.Common.DbConnection connection)
        {
            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT COUNT(*)
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = @database
                        AND TABLE_NAME = @tableName
                        AND INDEX_NAME = @indexName";

                System.Data.Common.DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = "@database";
                dbParam.Value = databaseName;
                command.Parameters.Add(dbParam);

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
                command.CommandText = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = @database";

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
