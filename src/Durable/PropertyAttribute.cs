namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Microsoft.Data.Sqlite;

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class PropertyAttribute : Attribute
    {
        public string Name { get; }
        public Flags PropertyFlags { get; }
        public int MaxLength { get; }

        public PropertyAttribute(string name, Flags flags = Flags.None, int maxLength = 0)
        {
            Name = name;
            PropertyFlags = flags;
            MaxLength = maxLength;
        }
    }
}