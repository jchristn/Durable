namespace Test.Shared
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Durable;

    /// <summary>
    /// Declares a foreign key for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapForeignKeyAttribute : Attribute
    {
        /// <summary>Gets the referenced class. Never null.</summary>
        [DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)]
        public Type Referenced { get; }

        /// <summary>Gets the referenced property. Never null.</summary>
        public string Property { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="referenced">Referenced class. Must not be null.</param>
        /// <param name="property">Referenced property. Must not be null.</param>
        public MapForeignKeyAttribute([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type referenced, string property)
        {
            Referenced = referenced ?? throw new ArgumentNullException(nameof(referenced));
            Property = property ?? throw new ArgumentNullException(nameof(property));
        }
    }
}
