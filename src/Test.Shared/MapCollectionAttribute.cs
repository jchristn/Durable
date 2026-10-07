namespace Test.Shared
{
    using System;

    /// <summary>
    /// Marks a one-to-many collection navigation for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapCollectionAttribute : Attribute
    {
        /// <summary>Gets the foreign key property on the related class. Never null.</summary>
        public string InverseForeignKeyProperty { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="inverseForeignKeyProperty">Foreign key property name on the related class. Must not be null.</param>
        public MapCollectionAttribute(string inverseForeignKeyProperty)
        {
            InverseForeignKeyProperty = inverseForeignKeyProperty ?? throw new ArgumentNullException(nameof(inverseForeignKeyProperty));
        }
    }
}
