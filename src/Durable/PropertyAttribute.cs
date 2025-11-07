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
    /// Attribute to specify database column properties for a field or property.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class PropertyAttribute : Attribute
    {
        /// <summary>
        /// Gets the name of the database column.
        /// </summary>
        public string Name { get; }
        
        /// <summary>
        /// Gets the property flags that define special behaviors for this property.
        /// </summary>
        public Flags PropertyFlags { get; }
        
        /// <summary>
        /// Gets the maximum length constraint for the property value.
        /// </summary>
        public int MaxLength { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PropertyAttribute"/> class.
        /// </summary>
        /// <param name="name">The name of the database column.</param>
        /// <param name="flags">The property flags (default is None).</param>
        /// <param name="maxLength">The maximum length constraint (default is 0 for no limit).</param>
        public PropertyAttribute(string name, Flags flags = Flags.None, int maxLength = 0)
        {
            Name = name;
            PropertyFlags = flags;
            MaxLength = maxLength;
        }
    }
}