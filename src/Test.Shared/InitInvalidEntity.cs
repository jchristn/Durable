namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Invalid test entity without proper attributes (for validation testing).
    /// Used by <see cref="InitializationTests"/>.
    /// </summary>
    public class InitInvalidEntity
    {
        /// <summary>
        /// Gets or sets a code. Not recognized as a key by convention, so the entity has no primary key.
        /// </summary>
        public int Code { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        public string Name { get; set; } = string.Empty;
    }
}
