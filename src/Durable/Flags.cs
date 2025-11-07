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
    /// Defines flags that can be applied to entity properties to specify their database characteristics.
    /// </summary>
    [Flags]
    public enum Flags
    {
        /// <summary>
        /// No flags applied.
        /// </summary>
        None = 0,
        /// <summary>
        /// Indicates that the property is a primary key.
        /// </summary>
        PrimaryKey = 1,
        /// <summary>
        /// Indicates that the property is a string type.
        /// </summary>
        String = 2,
        /// <summary>
        /// Indicates that the property value should auto-increment.
        /// </summary>
        AutoIncrement = 4
    }
}