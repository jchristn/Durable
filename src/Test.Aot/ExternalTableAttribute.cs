namespace Test.Aot
{
    using System;

    /// <summary>
    /// Stand-in for another library's table attribute; Durable never reads it, <see cref="ExternalMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class ExternalTableAttribute : Attribute
    {
        public ExternalTableAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }
}
