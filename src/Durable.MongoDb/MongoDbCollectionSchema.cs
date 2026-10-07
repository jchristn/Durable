namespace Durable.MongoDb
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using MongoDB.Bson;

    /// <summary>
    /// How an entity is stored in a MongoDB collection: the collection is <see cref="EntityMetadata.TableName"/>; each
    /// column is a document field named after the column, except a single primary key, which is stored as the document
    /// <c>_id</c>; a composite key is stored both as its fields and as an <c>_id</c> sub-document holding the key parts in
    /// key order (so the key is unique and every key lookup uses the <c>_id</c> index). Auto-increment is supported for a
    /// single integral primary key (generated from the backend's sequence collection). Also lists the indexes created for
    /// the entity: foreign keys, composite key parts, <see cref="IndexAttribute"/> columns (grouped by name, ordered by
    /// <see cref="IndexAttribute.Order"/>) and <see cref="CompositeIndexAttribute"/>s.
    /// Thread safety: immutable once built; <see cref="For"/> is thread-safe.
    /// </summary>
    internal sealed class MongoDbCollectionSchema
    {
        #region Public-Members

        /// <summary>
        /// The document field holding the primary key.
        /// </summary>
        public const string IdField = "_id";

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the collection name. Never null.
        /// </summary>
        public string CollectionName { get; }

        /// <summary>
        /// Gets whether the key is stored as the document <c>_id</c> directly (a single key column).
        /// </summary>
        public bool SingleKey { get; }

        /// <summary>
        /// Gets the stored type of generated keys (int or long), or null when the entity has no auto-increment column.
        /// </summary>
        public Type? GeneratedKeyType { get; }

        /// <summary>
        /// Gets the indexes of the entity (the <c>_id</c> index excluded). Never null.
        /// </summary>
        public IReadOnlyList<MongoDbIndexDefinition> Indexes { get; }

        #endregion

        #region Private-Members

        private static readonly ConcurrentDictionary<Type, MongoDbCollectionSchema> _Cache = new ConcurrentDictionary<Type, MongoDbCollectionSchema>();
        private readonly Dictionary<ColumnMetadata, string> _Fields = new Dictionary<ColumnMetadata, string>(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<string, string> _FieldsByProperty = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly Dictionary<ColumnMetadata, Type> _StoredTypes = new Dictionary<ColumnMetadata, Type>(ReferenceEqualityComparer.Instance);

        #endregion

        #region Constructors-and-Factories

        private MongoDbCollectionSchema(EntityMetadata metadata)
        {
            Metadata = metadata;
            CollectionName = metadata.TableName;
            ValidateCollectionName(metadata);
            metadata.RequireKey();
            SingleKey = metadata.KeyColumns.Count == 1;

            HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                string field = SingleKey && column.IsPrimaryKey ? IdField : column.Name;
                if (string.Equals(column.Name, IdField, StringComparison.Ordinal) && !(SingleKey && column.IsPrimaryKey))
                    throw new NotSupportedException("Column '" + column.Name + "' of " + metadata.EntityType.Name + " is reserved by MongoDB for the primary key; only a single primary key column may be named '_id'.");
                if (field != IdField) ValidateFieldName(metadata, column.Name);
                if (!seen.Add(field))
                    throw new NotSupportedException("Columns of " + metadata.EntityType.Name + " map to the same MongoDB field '" + field + "'.");
                _Fields[column] = field;
                _FieldsByProperty[column.Property.Name] = field;
                _StoredTypes[column] = MongoDbValueConverter.StoredType(column);
            }

            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            if (generated != null)
            {
                Type stored = _StoredTypes[generated];
                if (!SingleKey || !generated.IsPrimaryKey)
                    throw new NotSupportedException("The MongoDB backend generates auto-increment values only for a single primary key column; " + metadata.EntityType.Name + "." + generated.Property.Name + " is not one.");
                if (stored == typeof(long) || stored == typeof(uint)) GeneratedKeyType = typeof(long);
                else if (stored == typeof(int) || stored == typeof(short) || stored == typeof(byte) || stored == typeof(ushort) || stored == typeof(sbyte)) GeneratedKeyType = typeof(int);
                else throw new NotSupportedException("The MongoDB backend generates auto-increment values only for integer keys; " + metadata.EntityType.Name + "." + generated.Property.Name + " is " + stored.Name + ".");
            }

            Indexes = BuildIndexes(metadata);
        }

        /// <summary>
        /// Returns the cached schema of an entity.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <returns>The schema.</returns>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in MongoDB (invalid collection or field names, unsupported auto-increment).</exception>
        public static MongoDbCollectionSchema For(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            if (_Cache.TryGetValue(metadata.EntityType, out MongoDbCollectionSchema? cached)) return cached;
            MongoDbCollectionSchema schema = new MongoDbCollectionSchema(metadata);
            return _Cache.GetOrAdd(metadata.EntityType, schema);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the document field of a column.
        /// </summary>
        /// <param name="column">Column of this entity. Must not be null.</param>
        /// <returns>The field name.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the column does not belong to the entity.</exception>
        public string Field(ColumnMetadata column)
        {
            if (_Fields.TryGetValue(column, out string? field)) return field;
            if (_FieldsByProperty.TryGetValue(column.Property.Name, out field)) return field;
            throw new InvalidOperationException("Column '" + column.Name + "' is not mapped by " + Metadata.EntityType.Name + ".");
        }

        /// <summary>
        /// Returns the stored type of a column (see <see cref="MongoDbValueConverter.StoredType"/>).
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The stored type.</returns>
        public Type StoredType(ColumnMetadata column)
        {
            return _StoredTypes.TryGetValue(column, out Type? type) ? type : MongoDbValueConverter.StoredType(column);
        }

        /// <summary>
        /// Builds the document <c>_id</c> of a key.
        /// </summary>
        /// <param name="storedKey">Stored key values in key order. Must not be null; no part may be null.</param>
        /// <returns>The _id value.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a key part is null.</exception>
        public BsonValue Id(object?[] storedKey)
        {
            for (int i = 0; i < storedKey.Length; i++)
            {
                if (storedKey[i] == null)
                    throw new InvalidOperationException("Primary key column '" + Metadata.KeyColumns[i].Name + "' of " + Metadata.EntityType.Name + " is null.");
            }

            if (SingleKey) return MongoDbBsonCodec.Encode(storedKey[0]);
            BsonDocument id = new BsonDocument();
            for (int i = 0; i < storedKey.Length; i++) id[Metadata.KeyColumns[i].Name] = MongoDbBsonCodec.Encode(storedKey[i]);
            return id;
        }

        /// <summary>
        /// Returns the MongoDB <c>$type</c> alias of a stored type (for partial index filters).
        /// </summary>
        /// <param name="storedType">Stored type. Must not be null.</param>
        /// <returns>The alias, for example "string" or "decimal".</returns>
        public static string BsonTypeAlias(Type storedType)
        {
            if (storedType == typeof(string) || storedType == typeof(char)) return "string";
            if (storedType == typeof(bool)) return "bool";
            if (storedType == typeof(int) || storedType == typeof(short) || storedType == typeof(byte) || storedType == typeof(sbyte)
                || storedType == typeof(ushort) || storedType == typeof(DateOnly)) return "int";
            if (storedType == typeof(long) || storedType == typeof(uint) || storedType == typeof(TimeSpan) || storedType == typeof(TimeOnly)) return "long";
            if (storedType == typeof(decimal) || storedType == typeof(ulong) || storedType == typeof(DateTime) || storedType == typeof(DateTimeOffset)) return "decimal";
            if (storedType == typeof(double) || storedType == typeof(float)) return "double";
            if (storedType == typeof(Guid) || storedType == typeof(byte[])) return "binData";
            return "long";
        }

        #endregion

        #region Private-Methods

        private List<MongoDbIndexDefinition> BuildIndexes(EntityMetadata metadata)
        {
            List<MongoDbIndexDefinition> indexes = new List<MongoDbIndexDefinition>();
            HashSet<string> names = new HashSet<string>(StringComparer.Ordinal);
            HashSet<string> keySignatures = new HashSet<string>(StringComparer.Ordinal);

            void Add(string name, List<ColumnMetadata> columns, bool isUnique)
            {
                if (columns.Count == 0 || columns.Any(c => _Fields[c] == IdField && columns.Count == 1)) return;
                string signature = string.Join(",", columns.Select(c => _Fields[c]));
                if (!names.Add(name) || !keySignatures.Add(signature + (isUnique ? "!" : string.Empty))) return;
                indexes.Add(new MongoDbIndexDefinition(name, columns, isUnique));
            }

            Dictionary<string, List<KeyValuePair<int, ColumnMetadata>>> named = new Dictionary<string, List<KeyValuePair<int, ColumnMetadata>>>(StringComparer.Ordinal);
            Dictionary<string, bool> unique = new Dictionary<string, bool>(StringComparer.Ordinal);
            List<string> order = new List<string>();
            foreach (ColumnMetadata column in metadata.Columns)
            {
                foreach (IndexAttribute index in column.Indexes)
                {
                    string name = index.Name ?? ("idx_" + metadata.TableName + "_" + column.Name);
                    if (!named.TryGetValue(name, out List<KeyValuePair<int, ColumnMetadata>>? columns))
                    {
                        columns = new List<KeyValuePair<int, ColumnMetadata>>();
                        named[name] = columns;
                        order.Add(name);
                    }

                    columns.Add(new KeyValuePair<int, ColumnMetadata>(index.Order, column));
                    unique[name] = (unique.TryGetValue(name, out bool u) && u) || index.IsUnique;
                }
            }

            foreach (string name in order)
                Add(name, named[name].OrderBy(c => c.Key).Select(c => c.Value).ToList(), unique[name]);

            foreach (CompositeIndexAttribute composite in metadata.CompositeIndexes)
            {
                List<ColumnMetadata> columns = new List<ColumnMetadata>();
                foreach (string columnName in composite.ColumnNames)
                {
                    ColumnMetadata? column = metadata.FindColumnByName(columnName) ?? metadata.FindColumnByProperty(columnName);
                    if (column == null)
                        throw new InvalidOperationException("Composite index '" + composite.Name + "' of " + metadata.EntityType.Name + " names unknown column '" + columnName + "'.");
                    columns.Add(column);
                }

                Add(composite.Name, columns, composite.IsUnique);
            }

            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (_Fields[column] == IdField) continue;
                if (column.ForeignKey != null || (column.IsPrimaryKey && !SingleKey))
                    Add("ix_" + column.Name, new List<ColumnMetadata> { column }, false);
            }

            return indexes;
        }

        private static void ValidateCollectionName(EntityMetadata metadata)
        {
            string name = metadata.TableName;
            bool valid = name.Length > 0 && name.IndexOf('$') < 0 && name.IndexOf('\0') < 0
                && !name.StartsWith("system.", StringComparison.Ordinal) && !name.StartsWith(".", StringComparison.Ordinal);
            if (!valid)
                throw new NotSupportedException("Table name '" + name + "' of " + metadata.EntityType.Name + " is not a valid MongoDB collection name (it must not be empty, contain '$' or a null character, or start with 'system.' or '.').");
        }

        private static void ValidateFieldName(EntityMetadata metadata, string field)
        {
            if (field.Length == 0 || field.IndexOf('.') >= 0 || field.StartsWith("$", StringComparison.Ordinal) || field.IndexOf('\0') >= 0)
                throw new NotSupportedException("Column '" + field + "' of " + metadata.EntityType.Name + " is not a valid MongoDB field name (it must not be empty, contain '.' or a null character, or start with '$').");
        }

        #endregion
    }
}
