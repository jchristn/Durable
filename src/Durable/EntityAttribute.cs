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
    /// Specifies the database table name for an entity class.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class EntityAttribute : Attribute
    {
        /// <summary>
        /// Gets the name of the database table associated with the entity.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// Initializes a new instance of the EntityAttribute class.
        /// </summary>
        /// <param name="name">The name of the database table.</param>
        public EntityAttribute(string name) => Name = name;
    }
}