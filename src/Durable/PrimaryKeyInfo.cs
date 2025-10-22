namespace Durable
{
    using System.Reflection;

    /// <summary>
    /// Represents information about a primary key column and its corresponding property.
    /// </summary>
    public sealed class PrimaryKeyInfo
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

        /// <summary>
        /// Gets or sets the name of the primary key column in the database.
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the PropertyInfo for the primary key property on the entity.
        /// </summary>
        public PropertyInfo Property { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PrimaryKeyInfo"/> class.
        /// </summary>
        public PrimaryKeyInfo()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PrimaryKeyInfo"/> class.
        /// </summary>
        /// <param name="columnName">The name of the primary key column.</param>
        /// <param name="property">The PropertyInfo for the primary key property.</param>
        public PrimaryKeyInfo(string columnName, PropertyInfo property)
        {
            ColumnName = columnName;
            Property = property;
        }

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    }
}
