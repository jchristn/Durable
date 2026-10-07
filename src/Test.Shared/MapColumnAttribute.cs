namespace Test.Shared
{
    using System;

    /// <summary>
    /// Maps a property to a column for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapColumnAttribute : Attribute
    {
        /// <summary>Gets the column name. Never null.</summary>
        public string Name { get; }

        /// <summary>Gets or sets whether the column is (part of) the primary key. Default: false.</summary>
        public bool Key { get; set; }

        /// <summary>Gets or sets whether the database generates the value. Default: false.</summary>
        public bool Identity { get; set; }

        /// <summary>Gets or sets the position in a composite key. Default: 0.</summary>
        public int KeyOrder { get; set; }

        /// <summary>Gets or sets the maximum length; 0 for none. Default: 0.</summary>
        public int Length { get; set; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="name">Column name. Must not be null.</param>
        public MapColumnAttribute(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }
    }
}
