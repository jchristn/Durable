namespace Test.Shared
{
    using System;

    /// <summary>
    /// Marks a reference navigation for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapReferenceAttribute : Attribute
    {
        /// <summary>Gets the foreign key property on this class. Never null.</summary>
        public string ForeignKeyProperty { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="foreignKeyProperty">Foreign key property name. Must not be null.</param>
        public MapReferenceAttribute(string foreignKeyProperty)
        {
            ForeignKeyProperty = foreignKeyProperty ?? throw new ArgumentNullException(nameof(foreignKeyProperty));
        }
    }
}
