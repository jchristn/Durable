namespace Test.Shared
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Names the value converter of a property for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapConverterAttribute : Attribute
    {
        /// <summary>Gets the converter type. Never null.</summary>
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
        public Type Converter { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="converter">Converter type with a public parameterless constructor. Must not be null.</param>
        public MapConverterAttribute([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type converter)
        {
            Converter = converter ?? throw new ArgumentNullException(nameof(converter));
        }
    }
}
