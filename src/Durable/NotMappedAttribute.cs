namespace Durable
{
    using System;

    /// <summary>
    /// Excludes a property from convention-based mapping.
    /// Only meaningful for entities mapped by convention (no <see cref="PropertyAttribute"/> on any property).
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class NotMappedAttribute : Attribute
    {
    }
}
