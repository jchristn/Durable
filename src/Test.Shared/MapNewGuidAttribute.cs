namespace Test.Shared
{
    using System;

    /// <summary>
    /// Gives a Guid property a new value on insert when it is empty, for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapNewGuidAttribute : Attribute
    {
    }
}
