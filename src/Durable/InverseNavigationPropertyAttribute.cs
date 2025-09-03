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
    public class InverseNavigationPropertyAttribute : Attribute
    {
        public string InverseForeignKeyProperty { get; }

        public InverseNavigationPropertyAttribute(string inverseForeignKeyProperty)
        {
            InverseForeignKeyProperty = inverseForeignKeyProperty;
        }
    }
}