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
    public class ManyToManyNavigationPropertyAttribute : Attribute
    {
        public Type JunctionEntityType { get; }
        public string ThisEntityForeignKeyProperty { get; }
        public string RelatedEntityForeignKeyProperty { get; }

        public ManyToManyNavigationPropertyAttribute(
            Type junctionEntityType, 
            string thisEntityForeignKeyProperty, 
            string relatedEntityForeignKeyProperty)
        {
            JunctionEntityType = junctionEntityType;
            ThisEntityForeignKeyProperty = thisEntityForeignKeyProperty;
            RelatedEntityForeignKeyProperty = relatedEntityForeignKeyProperty;
        }
    }
}