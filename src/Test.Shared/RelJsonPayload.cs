namespace Test.Shared
{
    using System.Collections.Generic;

    /// <summary>
    /// Nested object serialized into a JSON column.
    /// </summary>
    public class RelJsonPayload
    {
        /// <summary>
        /// Gets or sets the label. May be null.
        /// </summary>
        public string? Label { get; set; }

        /// <summary>
        /// Gets or sets a numeric score.
        /// </summary>
        public int Score { get; set; }

        /// <summary>
        /// Gets or sets nested values. Never null.
        /// </summary>
        public List<string> Values { get; set; } = new List<string>();

        /// <summary>
        /// Gets or sets a nested child payload. May be null.
        /// </summary>
        public RelJsonPayload? Child { get; set; }
    }
}
