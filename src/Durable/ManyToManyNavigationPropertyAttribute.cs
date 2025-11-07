namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Attribute to mark a property as a many-to-many navigation property with junction entity configuration.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ManyToManyNavigationPropertyAttribute : Attribute
    {
        /// <summary>
        /// Gets the type of the junction entity that connects the two entities in the many-to-many relationship.
        /// </summary>
        public Type JunctionEntityType { get; }
        
        /// <summary>
        /// Gets the foreign key property name in the junction entity that references this entity.
        /// </summary>
        public string ThisEntityForeignKeyProperty { get; }
        
        /// <summary>
        /// Gets the foreign key property name in the junction entity that references the related entity.
        /// </summary>
        public string RelatedEntityForeignKeyProperty { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ManyToManyNavigationPropertyAttribute"/> class.
        /// </summary>
        /// <param name="junctionEntityType">The type of the junction entity.</param>
        /// <param name="thisEntityForeignKeyProperty">The foreign key property name for this entity in the junction table.</param>
        /// <param name="relatedEntityForeignKeyProperty">The foreign key property name for the related entity in the junction table.</param>
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