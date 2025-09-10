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

    /// <summary>
    /// Specifies that a property represents a foreign key relationship to another entity.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ForeignKeyAttribute : Attribute
    {
        /// <summary>
        /// Gets the type of the entity that this foreign key references.
        /// </summary>
        public Type ReferencedType { get; }
        /// <summary>
        /// Gets the name of the property in the referenced entity that this foreign key points to.
        /// </summary>
        public string ReferencedProperty { get; }

        /// <summary>
        /// Initializes a new instance of the ForeignKeyAttribute class.
        /// </summary>
        /// <param name="referencedType">The type of the entity that this foreign key references.</param>
        /// <param name="referencedProperty">The name of the property in the referenced entity that this foreign key points to.</param>
        public ForeignKeyAttribute(Type referencedType, string referencedProperty)
        {
            ReferencedType = referencedType;
            ReferencedProperty = referencedProperty;
        }
    }
}