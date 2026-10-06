namespace Test.Shared
{
    using System.Collections.Generic;

    /// <summary>
    /// Plain class used by the conflict-resolver unit tests (no mapping, no database).
    /// </summary>
    public class ResolverSample
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. May be null.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets a counter.
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// Gets or sets tags. Never null.
        /// </summary>
        public List<string> Tags { get; set; } = new List<string>();
    }
}
