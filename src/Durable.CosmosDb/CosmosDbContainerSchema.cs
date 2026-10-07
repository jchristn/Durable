namespace Durable.CosmosDb
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;
    using System.Text.Json.Nodes;
    using Durable;
    using Microsoft.Azure.Cosmos;

    /// <summary>
    /// How an entity is stored in a Cosmos DB container: the container is <see cref="EntityMetadata.TableName"/>; each column
    /// is a document property named after the column, except that a column whose name is reserved by Cosmos DB or Durable
    /// (<c>id</c>, <c>_rid</c>, <c>_self</c>, <c>_etag</c>, <c>_attachments</c>, <c>_ts</c>, <c>_lsn</c>, <c>_durable</c>) is
    /// stored as <c>durable_</c> plus its name. The document <c>id</c> is the primary key as text: a single primary key column
    /// named <c>id</c> whose values are strings (string, char, Guid, enum names, dates) is stored directly as the
    /// <c>id</c> (such keys cannot contain <c>/</c>, <c>\</c>, <c>?</c>, <c>#</c> or a percent escape like <c>%2F</c>); any
    /// other key is rendered as invariant text with <c>~</c>, <c>%</c>, <c>/</c>, <c>\</c>, <c>?</c>, <c>#</c> and <c>|</c>
    /// escaped as <c>~</c> plus two hex digits, composite key parts joined with <c>|</c> in key order. The partition key path is
    /// <c>/id</c> unless <see cref="CosmosDbRepositorySettings.PartitionKeys"/> names a column for the entity.
    /// Thread safety: immutable once built; safe for concurrent use.
    /// </summary>
    internal sealed class CosmosDbContainerSchema
    {
        #region Public-Members

        /// <summary>
        /// The document id property.
        /// </summary>
        public const string IdField = "id";

        /// <summary>
        /// The document property holding exact shadows of approximate values.
        /// </summary>
        public const string ShadowField = "_durable";

        /// <summary>
        /// The document property holding the ETag.
        /// </summary>
        public const string ETagField = "_etag";

        /// <summary>
        /// The longest document id Cosmos DB accepts, in characters.
        /// </summary>
        public const int MaxIdLength = 255;

        /// <summary>
        /// Gets the system properties Cosmos DB adds to every document.
        /// </summary>
        public static IReadOnlyList<string> SystemFields { get; } = new[] { "_rid", "_self", "_etag", "_attachments", "_ts", "_lsn" };

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the container name. Never null.
        /// </summary>
        public string ContainerName { get; }

        /// <summary>
        /// Gets whether the entity has a single key column.
        /// </summary>
        public bool SingleKey { get; }

        /// <summary>
        /// Gets whether the single key column is stored as the document id itself.
        /// </summary>
        public bool KeyInId { get; }

        /// <summary>
        /// Gets the partition key column, or null when the partition key is the document id.
        /// </summary>
        public ColumnMetadata? PartitionKeyColumn { get; }

        /// <summary>
        /// Gets the partition key path, for example <c>/id</c>. Never null.
        /// </summary>
        public string PartitionKeyPath { get; }

        /// <summary>
        /// Gets the document property of the partition key. Never null.
        /// </summary>
        public string PartitionKeyField { get; }

        /// <summary>
        /// Gets whether the partition key value follows from the primary key (the partition key is the id or a key column),
        /// so a key lookup can be a point read.
        /// </summary>
        public bool PartitionKeyFromKey { get; }

        #endregion

        #region Private-Members

        private static readonly HashSet<string> _Reserved = new HashSet<string>(StringComparer.Ordinal)
        {
            "id", "_rid", "_self", "_etag", "_attachments", "_ts", "_lsn", "_durable"
        };

        private readonly Dictionary<ColumnMetadata, string> _Fields = new Dictionary<ColumnMetadata, string>(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<string, string> _FieldsByProperty = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly Dictionary<ColumnMetadata, Type> _StoredTypes = new Dictionary<ColumnMetadata, Type>(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<ColumnMetadata, CosmosDbValueKind> _Kinds = new Dictionary<ColumnMetadata, CosmosDbValueKind>(ReferenceEqualityComparer.Instance);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Builds the schema of an entity.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="partitionKeyProperty">Property (or column) name of the partition key column; null for the document id.</param>
        /// <exception cref="ArgumentNullException">Thrown when metadata is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity has no primary key.</exception>
        /// <exception cref="NotSupportedException">Thrown when the entity cannot be stored in Cosmos DB (invalid container name, colliding property names, unsupported value types, partition key or auto-increment column).</exception>
        public CosmosDbContainerSchema(EntityMetadata metadata, string? partitionKeyProperty)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            Metadata = metadata;
            ContainerName = metadata.TableName;
            ValidateContainerName(metadata);
            metadata.RequireKey();
            SingleKey = metadata.KeyColumns.Count == 1;

            foreach (ColumnMetadata column in metadata.Columns)
            {
                Type stored = CosmosDbValueConverter.StoredType(column);
                CosmosDbValueKind kind;
                try
                {
                    kind = CosmosDbJsonCodec.KindOf(stored);
                }
                catch (NotSupportedException e)
                {
                    throw new NotSupportedException("Column '" + column.Name + "' of " + metadata.EntityType.Name + ": " + e.Message, e);
                }

                _StoredTypes[column] = stored;
                _Kinds[column] = kind;
            }

            ColumnMetadata firstKey = metadata.KeyColumns[0];
            KeyInId = SingleKey && string.Equals(firstKey.Name, IdField, StringComparison.Ordinal)
                && (_Kinds[firstKey] == CosmosDbValueKind.String || _Kinds[firstKey] == CosmosDbValueKind.Guid
                    || _Kinds[firstKey] == CosmosDbValueKind.DateTime || _Kinds[firstKey] == CosmosDbValueKind.DateTimeOffset
                    || _Kinds[firstKey] == CosmosDbValueKind.DateOnly);

            HashSet<string> seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                string field;
                if (KeyInId && ReferenceEquals(column, firstKey)) field = IdField;
                else if (_Reserved.Contains(column.Name)) field = "durable_" + column.Name;
                else field = column.Name;

                if (!seen.Add(field))
                    throw new NotSupportedException("Columns of " + metadata.EntityType.Name + " map to the same Cosmos DB property '" + field + "'.");
                _Fields[column] = field;
                _FieldsByProperty[column.Property.Name] = field;
            }

            if (partitionKeyProperty == null)
            {
                PartitionKeyColumn = null;
                PartitionKeyField = IdField;
                PartitionKeyFromKey = true;
            }
            else
            {
                ColumnMetadata? column = null;
                foreach (ColumnMetadata candidate in metadata.Columns)
                {
                    if (string.Equals(candidate.Property.Name, partitionKeyProperty, StringComparison.Ordinal)) column = candidate;
                }

                if (column == null)
                {
                    foreach (ColumnMetadata candidate in metadata.Columns)
                    {
                        if (string.Equals(candidate.Name, partitionKeyProperty, StringComparison.Ordinal)) column = candidate;
                    }
                }

                if (column == null)
                    throw new NotSupportedException("The partition key '" + partitionKeyProperty + "' configured for " + metadata.EntityType.Name + " is not a mapped property or column.");
                CosmosDbValueKind kind = _Kinds[column];
                if (kind == CosmosDbValueKind.Decimal || kind == CosmosDbValueKind.Single || kind == CosmosDbValueKind.Double || kind == CosmosDbValueKind.Bytes)
                    throw new NotSupportedException("Column '" + column.Name + "' of " + metadata.EntityType.Name + " cannot be the partition key: Cosmos DB partition keys must be exact strings, integers or booleans, not " + _StoredTypes[column].Name + ".");

                PartitionKeyField = _Fields[column];
                if (PartitionKeyField == IdField)
                {
                    PartitionKeyColumn = null;
                    PartitionKeyFromKey = true;
                }
                else
                {
                    PartitionKeyColumn = column;
                    PartitionKeyFromKey = column.IsPrimaryKey;
                }
            }

            PartitionKeyPath = "/" + PartitionKeyField;

            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            if (generated != null)
            {
                Type stored = _StoredTypes[generated];
                bool integral = stored == typeof(int) || stored == typeof(long) || stored == typeof(short) || stored == typeof(byte)
                    || stored == typeof(uint) || stored == typeof(ushort) || stored == typeof(sbyte) || stored == typeof(ulong);
                if (!integral)
                    throw new NotSupportedException("The Cosmos DB backend generates auto-increment values only for integer columns; " + metadata.EntityType.Name + "." + generated.Property.Name + " is " + stored.Name + ".");
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the document property of a column.
        /// </summary>
        /// <param name="column">Column of this entity. Must not be null.</param>
        /// <returns>The property name.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the column does not belong to the entity.</exception>
        public string Field(ColumnMetadata column)
        {
            if (_Fields.TryGetValue(column, out string? field)) return field;
            if (_FieldsByProperty.TryGetValue(column.Property.Name, out field)) return field;
            throw new InvalidOperationException("Column '" + column.Name + "' is not mapped by " + Metadata.EntityType.Name + ".");
        }

        /// <summary>
        /// Returns the stored type of a column (see <see cref="CosmosDbValueConverter.StoredType"/>).
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The stored type.</returns>
        public Type StoredType(ColumnMetadata column)
        {
            return _StoredTypes.TryGetValue(column, out Type? type) ? type : CosmosDbValueConverter.StoredType(column);
        }

        /// <summary>
        /// Returns the representation kind of a column.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The kind.</returns>
        public CosmosDbValueKind Kind(ColumnMetadata column)
        {
            return _Kinds.TryGetValue(column, out CosmosDbValueKind kind) ? kind : CosmosDbJsonCodec.KindOf(StoredType(column));
        }

        /// <summary>
        /// Returns the Cosmos DB SQL path of a column's property, for example <c>c["name"]</c>.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The path.</returns>
        public string Path(ColumnMetadata column)
        {
            return FieldPath(Field(column));
        }

        /// <summary>
        /// Returns the Cosmos DB SQL path of the shadow of a column, for example <c>c["_durable"]["price"]</c>.
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The path.</returns>
        public string ShadowPath(ColumnMetadata column)
        {
            return "c[" + Quote(ShadowField) + "][" + Quote(Field(column)) + "]";
        }

        /// <summary>
        /// Returns the Cosmos DB SQL path of a property.
        /// </summary>
        /// <param name="field">Property name. Must not be null.</param>
        /// <returns>The path.</returns>
        public static string FieldPath(string field)
        {
            return "c[" + Quote(field) + "]";
        }

        /// <summary>
        /// Builds the document id of a key.
        /// </summary>
        /// <param name="storedKey">Stored key values in key order. Must not be null; no part may be null.</param>
        /// <returns>The id.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a key part is null, or the id is empty, too long or contains characters Cosmos DB does not allow.</exception>
        public string Id(object?[] storedKey)
        {
            for (int i = 0; i < storedKey.Length; i++)
            {
                if (storedKey[i] == null)
                    throw new InvalidOperationException("Primary key column '" + Metadata.KeyColumns[i].Name + "' of " + Metadata.EntityType.Name + " is null.");
            }

            string id;
            if (KeyInId)
            {
                id = (string)CosmosDbJsonCodec.Parameter(storedKey[0]!);
                if (id.IndexOfAny(new[] { '/', '\\', '?', '#' }) >= 0 || HasPercentEscape(id))
                    throw new InvalidOperationException("The key '" + id + "' of " + Metadata.EntityType.Name + " cannot be a Cosmos DB id: ids cannot contain '/', '\\', '?', '#' or a percent escape such as '%2F'.");
            }
            else
            {
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < storedKey.Length; i++)
                {
                    if (i > 0) text.Append('|');
                    Escape(text, Part(storedKey[i]!));
                }

                id = text.ToString();
            }

            if (id.Length == 0) throw new InvalidOperationException("The key of " + Metadata.EntityType.Name + " cannot be empty: Cosmos DB ids must not be empty.");
            if (id.Length > MaxIdLength)
                throw new InvalidOperationException("The key of " + Metadata.EntityType.Name + " is too long for a Cosmos DB id (" + id.Length.ToString(CultureInfo.InvariantCulture) + " characters; at most " + MaxIdLength.ToString(CultureInfo.InvariantCulture) + ").");
            return id;
        }

        /// <summary>
        /// Returns the partition key of a document.
        /// </summary>
        /// <param name="document">Document. Must not be null.</param>
        /// <returns>The partition key.</returns>
        public PartitionKey PartitionKeyOf(JsonObject document)
        {
            JsonNode? node = document[PartitionKeyField];
            if (node == null) return PartitionKey.Null;
            System.Text.Json.JsonElement element = CosmosDbJsonCodec.ToElement(node);
            switch (element.ValueKind)
            {
                case System.Text.Json.JsonValueKind.String: return new PartitionKey(element.GetString());
                case System.Text.Json.JsonValueKind.Number: return new PartitionKey(element.GetDouble());
                case System.Text.Json.JsonValueKind.True: return new PartitionKey(true);
                case System.Text.Json.JsonValueKind.False: return new PartitionKey(false);
                default: return PartitionKey.Null;
            }
        }

        /// <summary>
        /// Returns the partition key for a stored value of the partition key column.
        /// </summary>
        /// <param name="stored">Stored value; may be null.</param>
        /// <returns>The partition key.</returns>
        public static PartitionKey PartitionKeyOfValue(object? stored)
        {
            if (stored == null) return PartitionKey.Null;
            object parameter = CosmosDbJsonCodec.Parameter(stored);
            switch (parameter)
            {
                case string text: return new PartitionKey(text);
                case bool flag: return new PartitionKey(flag);
                default: return new PartitionKey(Convert.ToDouble(parameter, CultureInfo.InvariantCulture));
            }
        }

        #endregion

        #region Private-Methods

        private static string Quote(string value)
        {
            StringBuilder text = new StringBuilder("\"", value.Length + 2);
            foreach (char c in value)
            {
                switch (c)
                {
                    case '"': text.Append("\\\""); break;
                    case '\\': text.Append("\\\\"); break;
                    case '\n': text.Append("\\n"); break;
                    case '\r': text.Append("\\r"); break;
                    case '\t': text.Append("\\t"); break;
                    default:
                        if (c < ' ') text.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        else text.Append(c);
                        break;
                }
            }

            return text.Append('"').ToString();
        }

        private static string Part(object stored)
        {
            object parameter = CosmosDbJsonCodec.Parameter(stored);
            switch (parameter)
            {
                case string text: return text;
                case bool flag: return flag ? "true" : "false";
                case double dbl: return dbl.ToString("R", CultureInfo.InvariantCulture);
                case decimal number: return number.ToString("G29", CultureInfo.InvariantCulture);
                case IFormattable formattable: return formattable.ToString(null, CultureInfo.InvariantCulture);
                default: return parameter.ToString() ?? string.Empty;
            }
        }

        private static void Escape(StringBuilder text, string part)
        {
            // '~' escapes (not '%': the gateway URL-decodes percent sequences in ids, so such ids cannot be point-read).
            foreach (char c in part)
            {
                switch (c)
                {
                    case '~': text.Append("~7E"); break;
                    case '%': text.Append("~25"); break;
                    case '/': text.Append("~2F"); break;
                    case '\\': text.Append("~5C"); break;
                    case '?': text.Append("~3F"); break;
                    case '#': text.Append("~23"); break;
                    case '|': text.Append("~7C"); break;
                    default: text.Append(c); break;
                }
            }
        }

        private static bool HasPercentEscape(string id)
        {
            for (int i = 0; i + 2 < id.Length; i++)
            {
                if (id[i] == '%' && Uri.IsHexDigit(id[i + 1]) && Uri.IsHexDigit(id[i + 2])) return true;
            }

            return false;
        }

        private static void ValidateContainerName(EntityMetadata metadata)
        {
            string name = metadata.TableName;
            bool valid = name.Length > 0 && name.Length <= 255 && !name.EndsWith(" ", StringComparison.Ordinal) && name.IndexOfAny(new[] { '/', '\\', '#', '?' }) < 0;
            if (!valid)
                throw new NotSupportedException("Table name '" + name + "' of " + metadata.EntityType.Name + " is not a valid Cosmos DB container name (1 to 255 characters, no '/', '\\', '#' or '?', no trailing space).");
        }

        #endregion
    }
}
