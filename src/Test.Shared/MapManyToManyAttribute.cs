namespace Test.Shared
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Durable;

    /// <summary>
    /// Marks a many-to-many navigation through a junction class for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapManyToManyAttribute : Attribute
    {
        /// <summary>Gets the junction class. Never null.</summary>
        [DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)]
        public Type Junction { get; }

        /// <summary>Gets the junction property referencing this class. Never null.</summary>
        public string ThisKey { get; }

        /// <summary>Gets the junction property referencing the related class. Never null.</summary>
        public string OtherKey { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="junction">Junction class. Must not be null.</param>
        /// <param name="thisKey">Junction property referencing this class. Must not be null.</param>
        /// <param name="otherKey">Junction property referencing the related class. Must not be null.</param>
        public MapManyToManyAttribute([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type junction, string thisKey, string otherKey)
        {
            Junction = junction ?? throw new ArgumentNullException(nameof(junction));
            ThisKey = thisKey ?? throw new ArgumentNullException(nameof(thisKey));
            OtherKey = otherKey ?? throw new ArgumentNullException(nameof(otherKey));
        }
    }
}
