namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Invalid test entity with attributes but no primary key (for validation testing).
    /// Used by <see cref="InitializationTests"/>.
    /// </summary>
    [Entity("invalid_no_pk")]
    public class InitInvalidNoPrimaryKey
    {
        /// <summary>
        /// Gets or sets the ID.
        /// </summary>
        [Property("id")]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        [Property("name")]
        public string Name { get; set; } = string.Empty;
    }
}
