namespace Test.Shared
{
    using System;

    /// <summary>
    /// Declares a single-column index for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapIndexAttribute : Attribute
    {
        /// <summary>Gets the index name. Never null.</summary>
        public string Name { get; }

        /// <summary>Gets or sets whether the index is unique. Default: false.</summary>
        public bool Unique { get; set; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="name">Index name. Must not be null.</param>
        public MapIndexAttribute(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }
    }
}
