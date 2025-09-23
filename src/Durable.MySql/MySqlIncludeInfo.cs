namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Contains metadata about navigation properties for Include operations in MySQL queries.
    /// Supports both single-entity and collection navigation properties with foreign key relationships.
    /// </summary>
    internal class MySqlIncludeInfo
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the full property path for this include (e.g., "Company.Address").
        /// </summary>
        public string PropertyPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the navigation property that will be populated with related data.
        /// </summary>
        public PropertyInfo NavigationProperty { get; set; } = null!;

        /// <summary>
        /// Gets or sets the foreign key property that establishes the relationship.
        /// </summary>
        public PropertyInfo? ForeignKeyProperty { get; set; }

        /// <summary>
        /// Gets or sets the name of the inverse foreign key property for collection navigation properties.
        /// </summary>
        public string? InverseForeignKeyProperty { get; set; }

        /// <summary>
        /// Gets or sets the type of the related entity being included.
        /// </summary>
        public Type RelatedEntityType { get; set; } = null!;

        /// <summary>
        /// Gets or sets the database table name for the related entity.
        /// </summary>
        public string RelatedTableName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the SQL alias used for the related table in JOIN operations.
        /// </summary>
        public string JoinAlias { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the parent include info for nested includes (ThenInclude scenarios).
        /// </summary>
        public MySqlIncludeInfo? Parent { get; set; }

        /// <summary>
        /// Gets or sets the collection of child includes for nested navigation properties.
        /// </summary>
        public List<MySqlIncludeInfo> Children { get; set; } = new List<MySqlIncludeInfo>();

        /// <summary>
        /// Gets or sets whether this navigation property represents a collection (one-to-many relationship).
        /// </summary>
        public bool IsCollection { get; set; }

        /// <summary>
        /// Gets or sets whether this represents a many-to-many relationship requiring a junction table.
        /// </summary>
        public bool IsManyToMany { get; set; }

        /// <summary>
        /// Gets or sets the junction entity type for many-to-many relationships.
        /// </summary>
        public Type? JunctionEntityType { get; set; }

        /// <summary>
        /// Gets or sets the junction table name for many-to-many relationships.
        /// </summary>
        public string? JunctionTableName { get; set; }

        /// <summary>
        /// Gets or sets the SQL alias for the junction table in many-to-many scenarios.
        /// </summary>
        public string? JunctionAlias { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlIncludeInfo class.
        /// </summary>
        public MySqlIncludeInfo()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string representation of this include info for debugging purposes.
        /// </summary>
        /// <returns>A string containing the property path and related entity type information.</returns>
        public override string ToString()
        {
            return $"Include: {PropertyPath} -> {RelatedEntityType?.Name} (Alias: {JoinAlias})";
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}