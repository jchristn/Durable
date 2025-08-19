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

    [AttributeUsage(AttributeTargets.Property)]
    public class ForeignKeyAttribute : Attribute
    {
        public Type ReferencedType { get; }
        public string ReferencedProperty { get; }

        public ForeignKeyAttribute(Type referencedType, string referencedProperty)
        {
            ReferencedType = referencedType;
            ReferencedProperty = referencedProperty;
        }
    }
}