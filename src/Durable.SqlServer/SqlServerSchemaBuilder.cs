#nullable enable

namespace Durable.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Helper class for building SQL Server schema (CREATE TABLE) SQL from entity metadata
    /// </summary>
    internal class SqlServerSchemaBuilder
    {
        private readonly ISanitizer _sanitizer;
        private readonly IDataTypeConverter _dataTypeConverter;

        /// <summary>
        /// Initializes a new instance of the SqlServerSchemaBuilder class
        /// </summary>
        /// <param name="sanitizer">The sanitizer for SQL identifiers</param>
        /// <param name="dataTypeConverter">The data type converter for SQL type mapping</param>
        public SqlServerSchemaBuilder(ISanitizer sanitizer, IDataTypeConverter dataTypeConverter)
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
            sql.Append("IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'");
            sql.Append(_sanitizer.SanitizeIdentifier(tableName));
            sql.Append("') AND type in (N'U'))");
            sql.AppendLine();
            sql.Append("BEGIN");
            sql.AppendLine();
            sql.Append("CREATE TABLE ");
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
            sql.AppendLine();
            sql.Append("END");

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

            // Primary key and Auto-increment
            isPrimaryKey = (propAttr.PropertyFlags & Flags.PrimaryKey) == Flags.PrimaryKey;
            bool hasAutoIncrement = (propAttr.PropertyFlags & Flags.AutoIncrement) == Flags.AutoIncrement;

            if (isPrimaryKey)
            {
                if (hasAutoIncrement)
                {
                    // SQL Server uses IDENTITY for auto-increment
                    columnDef.Append(" IDENTITY(1,1)");
                }
                columnDef.Append(" PRIMARY KEY");
            }

            // Nullable (SQL Server defaults to nullable, so only specify NOT NULL if required)
            Type propertyType = property.PropertyType;
            bool isNullableType = propertyType.IsGenericType &&
                                  propertyType.GetGenericTypeDefinition() == typeof(Nullable<>);
            bool isReferenceType = !propertyType.IsValueType;

            // A column is NOT NULL if:
            // 1. It's a non-nullable value type (int, bool, DateTime, etc.) AND not explicitly marked as nullable
            // 2. Primary keys and IDENTITY columns are implicitly NOT NULL in SQL Server, but we'll be explicit
            if (!isNullableType && !isReferenceType)
            {
                // For value types that aren't Nullable<T>, default to NOT NULL unless there's a default value
                DefaultValueAttribute? defaultAttr = property.GetCustomAttribute<DefaultValueAttribute>();
                if (defaultAttr == null || isPrimaryKey)
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
        public static List<ColumnInfo> GetTableColumns(string tableName, string databaseName, System.Data.Common.DbConnection connection)
        {
            List<ColumnInfo> columns = new List<ColumnInfo>();

            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.IS_NULLABLE,
                        c.COLUMN_DEFAULT,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN (
                        SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                            ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                            AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                            AND tc.TABLE_CATALOG = ku.TABLE_CATALOG
                            AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                            AND tc.TABLE_NAME = ku.TABLE_NAME
                    ) pk ON c.TABLE_CATALOG = pk.TABLE_CATALOG
                        AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                        AND c.TABLE_NAME = pk.TABLE_NAME
                        AND c.COLUMN_NAME = pk.COLUMN_NAME
                    WHERE c.TABLE_CATALOG = @database AND c.TABLE_NAME = @tableName
                    ORDER BY c.ORDINAL_POSITION";

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
                        string columnName = reader.GetString(0);
                        string dataType = reader.GetString(1);
                        string isNullable = reader.GetString(2);
                        object? defaultValue = reader.IsDBNull(3) ? null : reader.GetValue(3);
                        bool isPrimaryKey = reader.GetInt32(4) == 1;

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
        public static bool TableExists(string tableName, string databaseName, System.Data.Common.DbConnection connection)
        {
            using (System.Data.Common.DbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @database AND TABLE_NAME = @tableName";

                System.Data.Common.DbParameter dbParam = command.CreateParameter();
                dbParam.ParameterName = "@database";
                dbParam.Value = databaseName;
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
                sql.Append("IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '");
                sql.Append(indexName);
                sql.Append("' AND object_id = OBJECT_ID('");
                sql.Append(_sanitizer.SanitizeIdentifier(tableName));
                sql.Append("'))");
                sql.AppendLine();
                sql.Append("BEGIN");
                sql.AppendLine();
                sql.Append("    CREATE ");
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
                sql.AppendLine();
                sql.Append("END");

                indexSqlStatements.Add(sql.ToString());
            }

            // Build indexes from CompositeIndexAttribute on class
            CompositeIndexAttribute[] compositeIndexAttrs = entityType.GetCustomAttributes<CompositeIndexAttribute>().ToArray();
            foreach (CompositeIndexAttribute compositeAttr in compositeIndexAttrs)
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '");
                sql.Append(compositeAttr.Name);
                sql.Append("' AND object_id = OBJECT_ID('");
                sql.Append(_sanitizer.SanitizeIdentifier(tableName));
                sql.Append("'))");
                sql.AppendLine();
                sql.Append("BEGIN");
                sql.AppendLine();
                sql.Append("    CREATE ");
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
                sql.AppendLine();
                sql.Append("END");

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
                        i.name,
                        i.is_unique
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON i.object_id = t.object_id
                    WHERE t.name = @tableName
                        AND i.is_primary_key = 0
                        AND i.type > 0
                    ORDER BY i.name";

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

                using (System.Data.Common.DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        bool isUnique = reader.GetBoolean(1);

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
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON i.object_id = t.object_id
                    WHERE i.name = @indexName
                        AND t.name = @tableName";

                System.Data.Common.DbParameter nameParam = command.CreateParameter();
                nameParam.ParameterName = "@indexName";
                nameParam.Value = indexName;
                command.Parameters.Add(nameParam);

                System.Data.Common.DbParameter tableParam = command.CreateParameter();
                tableParam.ParameterName = "@tableName";
                tableParam.Value = tableName;
                command.Parameters.Add(tableParam);

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
                command.CommandText = "SELECT COUNT(*) FROM sys.databases WHERE name = @database";

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
