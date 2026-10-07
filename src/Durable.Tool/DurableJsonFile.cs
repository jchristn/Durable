namespace Durable.Tool
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// The contents of a <c>durable.json</c> settings file. Every property is optional and supplies a default for the
    /// matching command-line option; relative paths are resolved against the file's directory.
    /// </summary>
    internal sealed class DurableJsonFile
    {
        /// <summary>Gets or sets the JSON schema reference (ignored).</summary>
        [JsonPropertyName("$schema")]
        public string? Schema { get; set; }

        /// <summary>Gets or sets the provider (--provider).</summary>
        [JsonPropertyName("provider")]
        public string? Provider { get; set; }

        /// <summary>Gets or sets the connection string (--connection).</summary>
        [JsonPropertyName("connection")]
        public string? Connection { get; set; }

        /// <summary>Gets or sets the project (--project).</summary>
        [JsonPropertyName("project")]
        public string? Project { get; set; }

        /// <summary>Gets or sets the assembly (--assembly).</summary>
        [JsonPropertyName("assembly")]
        public string? Assembly { get; set; }

        /// <summary>Gets or sets the target framework (--framework).</summary>
        [JsonPropertyName("framework")]
        public string? Framework { get; set; }

        /// <summary>Gets or sets the build configuration (--configuration).</summary>
        [JsonPropertyName("configuration")]
        public string? Configuration { get; set; }

        /// <summary>Gets or sets the migration discovery namespace (--migrations-namespace).</summary>
        [JsonPropertyName("migrationsNamespace")]
        public string? MigrationsNamespace { get; set; }

        /// <summary>Gets or sets the entity discovery namespace (--entities-namespace).</summary>
        [JsonPropertyName("entitiesNamespace")]
        public string? EntitiesNamespace { get; set; }

        /// <summary>Gets or sets the explicit entity type names (--entities).</summary>
        [JsonPropertyName("entities")]
        public List<string>? Entities { get; set; }

        /// <summary>Gets or sets the mapping source type name (--mapping-source).</summary>
        [JsonPropertyName("mappingSource")]
        public string? MappingSource { get; set; }

        /// <summary>Gets or sets the migration history table (--history-table).</summary>
        [JsonPropertyName("historyTable")]
        public string? HistoryTable { get; set; }
    }
}
