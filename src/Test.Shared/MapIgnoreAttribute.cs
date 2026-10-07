namespace Test.Shared
{
    using System;

    /// <summary>
    /// Excludes a property of a convention-mapped class for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapIgnoreAttribute : Attribute
    {
    }
}
