namespace Durable
{
    using System;

    /// <summary>
    /// Attribute to mark a property as a version column for concurrency control
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class VersionColumnAttribute : Attribute
    {
        /// <summary>
        /// Gets the type of version column
        /// </summary>
        public VersionColumnType Type { get; }
        
        /// <summary>
        /// Initializes a new instance of the VersionColumnAttribute class
        /// </summary>
        /// <param name="type">The type of version column</param>
        public VersionColumnAttribute(VersionColumnType type = VersionColumnType.RowVersion)
        {
            Type = type;
        }
    }
}