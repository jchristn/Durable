namespace Test.Shared
{
    using System;

    /// <summary>
    /// Marks the soft-delete column for <see cref="MapAttributeMappingSource"/>.
    /// Test-only: Durable never reads it; <see cref="MapAttributeMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class MapSoftDeleteAttribute : Attribute
    {
    }
}
