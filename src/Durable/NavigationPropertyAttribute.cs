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
    /// Attribute to mark a property as a navigation property and specify its foreign key property.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class NavigationPropertyAttribute : Attribute
    {
        /// <summary>
        /// Gets the name of the foreign key property that establishes the relationship.
        /// </summary>
        public string ForeignKeyProperty { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="NavigationPropertyAttribute"/> class.
        /// </summary>
        /// <param name="foreignKeyProperty">The name of the foreign key property.</param>
        public NavigationPropertyAttribute(string foreignKeyProperty)
        {
            ForeignKeyProperty = foreignKeyProperty;
        }
    }
}