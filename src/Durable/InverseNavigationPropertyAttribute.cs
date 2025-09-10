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
    /// Specifies the inverse navigation property for a relationship in the entity framework.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class InverseNavigationPropertyAttribute : Attribute
    {
        /// <summary>
        /// Gets the name of the foreign key property on the inverse side of the relationship.
        /// </summary>
        public string InverseForeignKeyProperty { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="InverseNavigationPropertyAttribute"/> class.
        /// </summary>
        /// <param name="inverseForeignKeyProperty">The name of the foreign key property on the inverse side of the relationship.</param>
        public InverseNavigationPropertyAttribute(string inverseForeignKeyProperty)
        {
            InverseForeignKeyProperty = inverseForeignKeyProperty;
        }
    }
}