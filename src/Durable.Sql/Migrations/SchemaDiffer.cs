namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Durable;

    /// <summary>
    /// Compares entity mappings (<see cref="EntityMetadata"/>) with a live schema and produces a <see cref="SchemaDiff"/>.
    /// The comparison is a pure function of its inputs: it performs no I/O, so it can be unit-tested and used to generate
    /// scripts. All SQL is rendered through the <see cref="ISqlDialect"/>.
    /// <para>
    /// Produced operations: create missing tables (with their indexes), add missing columns, create missing indexes, and
    /// (destructive) drop unmapped indexes and columns and re-create indexes whose definition changed.
    /// Reported only (never applied): type, max length, nullability and primary key differences, NOT NULL columns without a
    /// derivable default (see <see cref="SchemaSyncOptions"/>), and changes the dialect cannot express.
    /// </para>
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public static class SchemaDiffer
    {
        #region Public-Methods

        /// <summary>
        /// Compares entity types with the live tables.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="entityTypes">Entity types. Must not be null; must not contain two types mapping the same table.</param>
        /// <param name="liveTables">Existing tables keyed by table name (case-insensitive lookup); a missing key means the
        /// table does not exist. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>The diff.</returns>
        /// <exception cref="ArgumentNullException">Thrown when dialect, entityTypes or liveTables is null.</exception>
        /// <exception cref="ArgumentException">Thrown when two entity types map the same table.</exception>
        /// <exception cref="InvalidOperationException">Thrown when an entity has no primary key or no mapped columns.</exception>
        public static SchemaDiff Compare(ISqlDialect dialect, IEnumerable<Type> entityTypes, IReadOnlyDictionary<string, TableSchema> liveTables, SchemaSyncOptions? options = null)
        {
            ArgumentNullException.ThrowIfNull(dialect);
            ArgumentNullException.ThrowIfNull(entityTypes);
            ArgumentNullException.ThrowIfNull(liveTables);
            options ??= new SchemaSyncOptions();

            Dictionary<string, TableSchema> live = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
            foreach (KeyValuePair<string, TableSchema> pair in liveTables) live[pair.Key] = pair.Value;

            List<MigrationOperation> operations = new List<MigrationOperation>();
            List<SchemaDifference> differences = new List<SchemaDifference>();
            HashSet<string> tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (Type type in entityTypes.Distinct())
            {
                EntityMetadata metadata = EntityMetadata.For(type);
                if (metadata.KeyColumns.Count == 0) throw new InvalidOperationException("Entity " + type.Name + " has no primary key.");
                if (metadata.Columns.Count == 0) throw new InvalidOperationException("Entity " + type.Name + " has no mapped columns.");
                if (!tables.Add(metadata.TableName))
                    throw new ArgumentException("More than one entity type maps table '" + metadata.TableName + "'.", nameof(entityTypes));

                IReadOnlyList<IndexSchema> expectedIndexes = GetExpectedIndexes(metadata);
                if (!live.TryGetValue(metadata.TableName, out TableSchema? table))
                {
                    SqlStatementBuilder builder = new SqlStatementBuilder(dialect);
                    dialect.AppendCreateTable(builder, metadata);
                    operations.Add(new MigrationOperation(
                        MigrationOperationKind.CreateTable, metadata.TableName, null, null, false,
                        "Create table " + metadata.TableName, new[] { builder.Build() }));
                    foreach (IndexSchema index in expectedIndexes)
                        operations.Add(CreateIndexOperation(dialect, metadata.TableName, index, false, "Create index " + index + " on " + metadata.TableName));
                    continue;
                }

                HashSet<string> availableColumns = new HashSet<string>(table.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
                CompareColumns(dialect, metadata, table, options, operations, differences, availableColumns);
                CompareIndexes(dialect, metadata, table, expectedIndexes, options, operations, availableColumns);
            }

            return new SchemaDiff(dialect, operations.OrderBy(o => (int)o.Kind), differences);
        }

        /// <summary>
        /// Returns the secondary indexes an entity declares through <see cref="IndexAttribute"/> (default name
        /// "idx_{table}_{column}"; attributes sharing a name form one index ordered by <see cref="IndexAttribute.Order"/>)
        /// and <see cref="CompositeIndexAttribute"/>.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The expected indexes.</returns>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        public static IReadOnlyList<IndexSchema> GetExpectedIndexes(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            List<string> order = new List<string>();
            Dictionary<string, List<KeyValuePair<int, string>>> columns = new Dictionary<string, List<KeyValuePair<int, string>>>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, bool> unique = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string[]?> included = new Dictionary<string, string[]?>(StringComparer.OrdinalIgnoreCase);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                foreach (IndexAttribute index in column.Indexes)
                {
                    string name = index.Name ?? ("idx_" + metadata.TableName + "_" + column.Name);
                    if (!columns.TryGetValue(name, out List<KeyValuePair<int, string>>? list))
                    {
                        list = new List<KeyValuePair<int, string>>();
                        columns[name] = list;
                        order.Add(name);
                    }

                    list.Add(new KeyValuePair<int, string>(index.Order, column.Name));
                    unique[name] = (unique.TryGetValue(name, out bool u) && u) || index.IsUnique;
                    if (index.IncludedColumns != null) included[name] = index.IncludedColumns;
                }
            }

            List<IndexSchema> result = new List<IndexSchema>();
            foreach (string name in order)
            {
                included.TryGetValue(name, out string[]? include);
                result.Add(new IndexSchema(name, columns[name].OrderBy(c => c.Key).Select(c => c.Value), unique[name], include));
            }

            foreach (CompositeIndexAttribute composite in metadata.CompositeIndexes)
            {
                if (result.Any(i => string.Equals(i.Name, composite.Name, StringComparison.OrdinalIgnoreCase))) continue;
                IEnumerable<string> names = composite.ColumnNames.Select(n => (metadata.FindColumnByName(n) ?? metadata.FindColumnByProperty(n))?.Name ?? n);
                result.Add(new IndexSchema(composite.Name, names, composite.IsUnique, composite.IncludedColumns));
            }

            return result;
        }

        /// <summary>
        /// Returns the SQL literal used as DEFAULT when a NOT NULL column is added to an existing table, following the rule
        /// documented on <see cref="SchemaSyncOptions"/>, or null when no default can be derived.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>The literal, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when dialect or column is null.</exception>
        public static string? GetDefaultLiteral(ISqlDialect dialect, ColumnMetadata column, SchemaSyncOptions? options = null)
        {
            ArgumentNullException.ThrowIfNull(dialect);
            ArgumentNullException.ThrowIfNull(column);
            options ??= new SchemaSyncOptions();
            object? value = null;
            bool found = false;
            DefaultValueAttribute? attribute = column.DefaultValue?.Attribute;
            if (attribute != null)
            {
                switch (attribute.ValueType)
                {
                    case DefaultValueType.StaticValue:
                        value = attribute.StaticValue;
                        found = value != null;
                        break;
                    case DefaultValueType.Zero:
                        value = ZeroOf(column.ClrType);
                        found = value != null;
                        break;
                    case DefaultValueType.EmptyString:
                        value = string.Empty;
                        found = true;
                        break;
                    case DefaultValueType.EmptyGuid:
                        value = Guid.Empty;
                        found = true;
                        break;
                    case DefaultValueType.True:
                        value = true;
                        found = true;
                        break;
                    case DefaultValueType.False:
                        value = false;
                        found = true;
                        break;
                }
            }

            if (!found && options.UseClrDefaultsForNotNullColumns && column.Converter == null && !column.IsJson && IsClrDefaultable(column.ClrType))
            {
                value = Activator.CreateInstance(column.ClrType);
                if (column.ClrType.IsEnum && column.EnumAsString && !Enum.IsDefined(column.ClrType, value!)) return null;
                found = true;
            }

            if (!found) return null;
            try
            {
                return dialect.FormatLiteral(dialect.Converter.ConvertToDatabase(value, column));
            }
            catch (NotSupportedException)
            {
                return null;
            }
            catch (InvalidCastException)
            {
                return null;
            }
            catch (FormatException)
            {
                return null;
            }
        }

        #endregion

        #region Private-Methods

        private static void CompareColumns(
            ISqlDialect dialect,
            EntityMetadata metadata,
            TableSchema table,
            SchemaSyncOptions options,
            List<MigrationOperation> operations,
            List<SchemaDifference> differences,
            HashSet<string> availableColumns)
        {
            string tableName = metadata.TableName;
            foreach (ColumnMetadata column in metadata.Columns)
            {
                ColumnSchema? existing = table.FindColumn(column.Name);
                if (existing == null)
                {
                    AddMissingColumn(dialect, tableName, column, options, operations, differences, availableColumns);
                    continue;
                }

                string qualified = tableName + "." + column.Name;
                string expectedType = dialect.GetColumnType(column);
                string expectedCanonical = dialect.NormalizeColumnType(expectedType);
                string actualCanonical = dialect.NormalizeColumnType(existing.DataType);
                if (!string.Equals(expectedCanonical, actualCanonical, StringComparison.Ordinal))
                {
                    bool lengthOnly = IsLengthDifference(expectedCanonical, actualCanonical);
                    differences.Add(new SchemaDifference(
                        lengthOnly ? SchemaDifferenceKind.MaxLengthMismatch : SchemaDifferenceKind.TypeMismatch,
                        tableName, column.Name, expectedType, existing.DataType,
                        "Column " + qualified + " is " + existing.DataType + " but property " + metadata.EntityType.Name + "." + column.Property.Name +
                        " maps to " + expectedType + "; change the column " + (lengthOnly ? "length" : "type") + " in a migration."));
                }

                if (!column.IsPrimaryKey && !existing.IsPrimaryKey && column.IsNullable != existing.IsNullable)
                {
                    differences.Add(new SchemaDifference(
                        SchemaDifferenceKind.NullabilityMismatch, tableName, column.Name,
                        column.IsNullable ? "NULL" : "NOT NULL", existing.IsNullable ? "NULL" : "NOT NULL",
                        "Column " + qualified + " is " + (existing.IsNullable ? "NULL" : "NOT NULL") + " but property " + metadata.EntityType.Name + "." +
                        column.Property.Name + " is " + (column.IsNullable ? "nullable" : "non-nullable") + "; alter the column (backfilling nulls first) in a migration."));
                }

                if (column.IsPrimaryKey != existing.IsPrimaryKey)
                {
                    differences.Add(new SchemaDifference(
                        SchemaDifferenceKind.PrimaryKeyMismatch, tableName, column.Name,
                        column.IsPrimaryKey ? "primary key" : "not a key", existing.IsPrimaryKey ? "primary key" : "not a key",
                        "Column " + qualified + " is " + (existing.IsPrimaryKey ? string.Empty : "not ") + "part of the primary key but the mapping " +
                        (column.IsPrimaryKey ? "declares" : "does not declare") + " it as a key; primary keys are never changed automatically."));
                }
            }

            if (!options.DropUnmappedColumns) return;
            foreach (ColumnSchema existing in table.Columns)
            {
                if (metadata.FindColumnByName(existing.Name) != null) continue;
                string qualified = tableName + "." + existing.Name;
                if (existing.IsPrimaryKey)
                {
                    differences.Add(new SchemaDifference(
                        SchemaDifferenceKind.PrimaryKeyMismatch, tableName, existing.Name, null, existing.DataType,
                        "Key column " + qualified + " is not mapped by " + metadata.EntityType.Name + "; primary keys are never changed automatically."));
                }
                else if (!dialect.SupportsDropColumn)
                {
                    differences.Add(new SchemaDifference(
                        SchemaDifferenceKind.UnsupportedChange, tableName, existing.Name, null, existing.DataType,
                        "Column " + qualified + " is not mapped, but " + dialect.RepositoryType.DisplayName + " cannot drop columns; rebuild the table in a migration."));
                }
                else
                {
                    operations.Add(new MigrationOperation(
                        MigrationOperationKind.DropColumn, tableName, existing.Name, null, true,
                        "Drop unmapped column " + qualified,
                        new[] { new SqlStatement(dialect.DropColumnSql(tableName, existing.Name)) }));
                }
            }
        }

        private static void AddMissingColumn(
            ISqlDialect dialect,
            string tableName,
            ColumnMetadata column,
            SchemaSyncOptions options,
            List<MigrationOperation> operations,
            List<SchemaDifference> differences,
            HashSet<string> availableColumns)
        {
            string qualified = tableName + "." + column.Name;
            string type = dialect.GetColumnType(column);
            if (column.IsPrimaryKey)
            {
                differences.Add(new SchemaDifference(
                    SchemaDifferenceKind.PrimaryKeyMismatch, tableName, column.Name, "primary key", null,
                    "Key column " + qualified + " is missing; primary keys are never changed automatically."));
                return;
            }

            if (column.IsAutoIncrement)
            {
                differences.Add(new SchemaDifference(
                    SchemaDifferenceKind.UnsupportedChange, tableName, column.Name, type, null,
                    "Auto-increment column " + qualified + " is missing; it cannot be added to an existing table automatically."));
                return;
            }

            if (column.IsNullable)
            {
                operations.Add(AddColumnOperation(dialect, tableName, column, true, null, "Add column " + qualified + " " + type + " NULL", null));
                availableColumns.Add(column.Name);
                return;
            }

            string? literal = GetDefaultLiteral(dialect, column, options);
            if (literal != null)
            {
                operations.Add(AddColumnOperation(dialect, tableName, column, false, literal, "Add column " + qualified + " " + type + " NOT NULL DEFAULT " + literal, null));
                availableColumns.Add(column.Name);
                return;
            }

            if (options.AddUnresolvableNotNullColumnsAsNullable)
            {
                operations.Add(AddColumnOperation(
                    dialect, tableName, column, true, null, "Add column " + qualified + " " + type + " NULL",
                    "Column " + qualified + " is NOT NULL in the mapping but no default could be derived; it was added as NULL. Backfill it and make it NOT NULL in a migration."));
                availableColumns.Add(column.Name);
                return;
            }

            differences.Add(new SchemaDifference(
                SchemaDifferenceKind.NotNullColumnWithoutDefault, tableName, column.Name, type + " NOT NULL", null,
                "Column " + qualified + " is NOT NULL and existing rows need a value, but no default could be derived. Add a constant [DefaultValue], " +
                "make the property nullable, set SchemaSyncOptions.AddUnresolvableNotNullColumnsAsNullable, or add and backfill the column in a migration."));
        }

        private static void CompareIndexes(
            ISqlDialect dialect,
            EntityMetadata metadata,
            TableSchema table,
            IReadOnlyList<IndexSchema> expectedIndexes,
            SchemaSyncOptions options,
            List<MigrationOperation> operations,
            HashSet<string> availableColumns)
        {
            string tableName = metadata.TableName;
            HashSet<string> expectedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (IndexSchema expected in expectedIndexes)
            {
                string name = Truncate(expected.Name, dialect.MaxIdentifierLength);
                expectedNames.Add(name);
                if (!expected.Columns.All(availableColumns.Contains)) continue;

                IndexSchema? existing = table.FindIndex(name);
                if (existing == null)
                {
                    operations.Add(CreateIndexOperation(dialect, tableName, expected, false, "Create index " + expected + " on " + tableName));
                }
                else if (!expected.HasSameDefinition(existing))
                {
                    operations.Add(new MigrationOperation(
                        MigrationOperationKind.DropIndex, tableName, null, existing.Name, true,
                        "Drop index " + existing + " on " + tableName + " (definition changed to " + expected + ")",
                        new[] { new SqlStatement(dialect.DropIndexSql(existing.Name, tableName)) }));
                    operations.Add(CreateIndexOperation(dialect, tableName, expected, true, "Re-create index " + expected + " on " + tableName));
                }
            }

            if (!options.DropUnmappedIndexes) return;
            foreach (IndexSchema existing in table.Indexes)
            {
                if (expectedNames.Contains(existing.Name)) continue;
                operations.Add(new MigrationOperation(
                    MigrationOperationKind.DropIndex, tableName, null, existing.Name, true,
                    "Drop undeclared index " + existing + " on " + tableName,
                    new[] { new SqlStatement(dialect.DropIndexSql(existing.Name, tableName)) }));
            }
        }

        private static MigrationOperation AddColumnOperation(ISqlDialect dialect, string tableName, ColumnMetadata column, bool nullable, string? literal, string description, string? warning)
        {
            return new MigrationOperation(
                MigrationOperationKind.AddColumn, tableName, column.Name, null, false, description,
                new[] { new SqlStatement(dialect.AddColumnSql(tableName, column, nullable, literal)) }, warning);
        }

        private static MigrationOperation CreateIndexOperation(ISqlDialect dialect, string tableName, IndexSchema index, bool destructive, string description)
        {
            IReadOnlyList<string>? included = index.IncludedColumns.Count > 0 ? index.IncludedColumns : null;
            return new MigrationOperation(
                MigrationOperationKind.CreateIndex, tableName, null, index.Name, destructive, description,
                new[] { new SqlStatement(dialect.CreateIndexSql(index.Name, tableName, index.Columns, index.IsUnique, included)) });
        }

        private static bool IsLengthDifference(string expected, string actual)
        {
            int expectedParen = expected.IndexOf('(');
            int actualParen = actual.IndexOf('(');
            if (expectedParen < 0 || actualParen < 0) return false;
            if (!string.Equals(expected.Substring(0, expectedParen), actual.Substring(0, actualParen), StringComparison.Ordinal)) return false;
            return IsLengthSuffix(expected.Substring(expectedParen)) && IsLengthSuffix(actual.Substring(actualParen));
        }

        private static bool IsLengthSuffix(string suffix)
        {
            if (suffix.Length < 3 || suffix[suffix.Length - 1] != ')') return false;
            string inner = suffix.Substring(1, suffix.Length - 2);
            return inner == "max" || int.TryParse(inner, NumberStyles.None, CultureInfo.InvariantCulture, out int _);
        }

        private static string Truncate(string name, int maxLength)
        {
            return maxLength > 0 && name.Length > maxLength ? name.Substring(0, maxLength) : name;
        }

        private static bool IsClrDefaultable(Type type)
        {
            return type.IsEnum || type == typeof(bool) || type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong)
                || type == typeof(float) || type == typeof(double) || type == typeof(decimal);
        }

        private static object? ZeroOf(Type type)
        {
            if (type.IsEnum) return Enum.ToObject(type, 0);
            if (!IsClrDefaultable(type) || type == typeof(bool)) return null;
            return Convert.ChangeType(0, type, CultureInfo.InvariantCulture);
        }

        #endregion
    }
}
