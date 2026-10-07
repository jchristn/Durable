namespace Test.Shared
{
    using System;

    /// <summary>
    /// Declares a multi-column index for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public sealed class MapCompositeIndexAttribute : Attribute
    {
        /// <summary>Gets the index name. Never null.</summary>
        public string Name { get; }

        /// <summary>Gets the column names in index order. Never null.</summary>
        public string[] Columns { get; }

        /// <summary>Gets or sets whether the index is unique. Default: false.</summary>
        public bool Unique { get; set; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="name">Index name. Must not be null.</param>
        /// <param name="columns">Column names. Must not be null.</param>
        public MapCompositeIndexAttribute(string name, params string[] columns)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Columns = columns ?? throw new ArgumentNullException(nameof(columns));
        }
    }
}
