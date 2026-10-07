namespace Durable.CosmosDb
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json.Nodes;
    using Durable;
    using Microsoft.Azure.Cosmos;

    /// <summary>
    /// A document read from (or about to be written to) a Cosmos DB container, with lazily decoded stored column values.
    /// Thread safety: not thread-safe; owned by one operation.
    /// </summary>
    internal sealed class CosmosDbDocument
    {
        #region Public-Members

        /// <summary>
        /// Gets the schema of the document's entity. Never null.
        /// </summary>
        public CosmosDbContainerSchema Schema { get; }

        /// <summary>
        /// Gets the document content (including system properties when read from Cosmos DB). Never null.
        /// </summary>
        public JsonObject Content { get; }

        /// <summary>
        /// Gets the document id. Never null.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Gets the ETag of the stored version, or null for a document that has not been read from Cosmos DB.
        /// </summary>
        public string? ETag { get; }

        /// <summary>
        /// Gets the partition key of the document.
        /// </summary>
        public PartitionKey PartitionKey { get; }

        #endregion

        #region Private-Members

        private readonly Dictionary<ColumnMetadata, object?> _Values = new Dictionary<ColumnMetadata, object?>(ReferenceEqualityComparer.Instance);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Wraps document content.
        /// </summary>
        /// <param name="schema">Schema. Must not be null.</param>
        /// <param name="content">Content. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the content has no string id.</exception>
        public CosmosDbDocument(CosmosDbContainerSchema schema, JsonObject content)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            Content = content ?? throw new ArgumentNullException(nameof(content));
            JsonNode? id = content[CosmosDbContainerSchema.IdField];
            Id = id is JsonValue value && value.TryGetValue(out string? text) && text != null
                ? text
                : (id != null ? CosmosDbJsonCodec.ToElement(id).GetString() : null) ?? throw new InvalidOperationException("A Cosmos DB document of '" + schema.ContainerName + "' has no string id.");
            JsonNode? etag = content[CosmosDbContainerSchema.ETagField];
            ETag = etag == null ? null : CosmosDbJsonCodec.ToElement(etag).GetString();
            PartitionKey = schema.PartitionKeyOf(content);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the stored value of a column (decoded from the document, preferring its exact shadow).
        /// </summary>
        /// <param name="column">Column of the document's entity. Must not be null.</param>
        /// <returns>The stored value, or null when the property is null or missing.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the property cannot be read as the column's stored type.</exception>
        public object? Get(ColumnMetadata column)
        {
            if (_Values.TryGetValue(column, out object? cached)) return cached;
            string field = Schema.Field(column);
            object? value = CosmosDbJsonCodec.Decode(Content[field], Shadow(field), Schema.StoredType(column));
            _Values[column] = value;
            return value;
        }

        /// <summary>
        /// Returns the shadow text of a property, or null.
        /// </summary>
        /// <param name="field">Property name. Must not be null.</param>
        /// <returns>The shadow, or null.</returns>
        public string? Shadow(string field)
        {
            if (Content[CosmosDbContainerSchema.ShadowField] is not JsonObject shadows) return null;
            JsonNode? node = shadows[field];
            return node == null ? null : CosmosDbJsonCodec.ToElement(node).GetString();
        }

        /// <summary>
        /// Returns whether Cosmos DB may order this document's value of a column differently from C#: strings with characters
        /// at or above U+D800 (code point versus UTF-16 order), integers and decimals that have an exact shadow (Cosmos DB
        /// may compare their double approximations), and non-finite floating point values (stored as strings).
        /// </summary>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>True when the position of this document in a Cosmos DB ordering is not guaranteed to match C#.</returns>
        public bool HasUncertainOrder(ColumnMetadata column)
        {
            CosmosDbValueKind kind = Schema.Kind(column);
            string field = Schema.Field(column);
            JsonNode? node = Content[field];
            if (node == null) return false;
            switch (kind)
            {
                case CosmosDbValueKind.String:
                    {
                        System.Text.Json.JsonElement element = CosmosDbJsonCodec.ToElement(node);
                        return element.ValueKind == System.Text.Json.JsonValueKind.String && CosmosDbJsonCodec.HasHighCharacters(element.GetString()!);
                    }
                case CosmosDbValueKind.Int64:
                case CosmosDbValueKind.Decimal:
                    return Shadow(field) != null;
                case CosmosDbValueKind.Single:
                case CosmosDbValueKind.Double:
                    return CosmosDbJsonCodec.ToElement(node).ValueKind == System.Text.Json.JsonValueKind.String;
                default:
                    return false;
            }
        }

        #endregion
    }
}
