namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Entity with explicit (<see cref="Flags.Json"/>) and implicit (non-scalar type) JSON columns.
    /// </summary>
    [Entity("rel_json_documents")]
    public class RelJsonDocument
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the explicitly JSON-mapped payload. May be null.
        /// </summary>
        [Property("payload", Flags.Json)]
        public RelJsonPayload? Payload { get; set; }

        /// <summary>
        /// Gets or sets an implicitly JSON-mapped nested object. May be null.
        /// </summary>
        [Property("nested")]
        public RelJsonPayload? Nested { get; set; }

        /// <summary>
        /// Gets or sets an implicitly JSON-mapped list. May be null.
        /// </summary>
        [Property("tags")]
        public List<string>? Tags { get; set; }

        /// <summary>
        /// Gets or sets an implicitly JSON-mapped dictionary. May be null.
        /// </summary>
        [Property("counters")]
        public Dictionary<string, int>? Counters { get; set; }
    }
}
