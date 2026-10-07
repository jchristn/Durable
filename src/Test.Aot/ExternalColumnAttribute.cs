namespace Test.Aot
{
    using System;

    /// <summary>
    /// Stand-in for another library's column attribute; Durable never reads it, <see cref="ExternalMappingSource"/> translates it.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ExternalColumnAttribute : Attribute
    {
        public ExternalColumnAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public bool Key { get; set; }

        public bool Isbn { get; set; }
    }
}
