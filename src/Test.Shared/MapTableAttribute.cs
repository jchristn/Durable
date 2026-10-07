namespace Test.Shared
{
    using System;

    /// <summary>
    /// Names the table of a class mapped by <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class MapTableAttribute : Attribute
    {
        /// <summary>Gets the table name. Never null.</summary>
        public string Name { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="name">Table name. Must not be null.</param>
        public MapTableAttribute(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }
    }
}
