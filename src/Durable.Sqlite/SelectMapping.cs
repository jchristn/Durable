namespace Durable.Sqlite
{
    using System.Reflection;

    /// <summary>
    /// Represents the mapping information for a column selection in SQL query projection.
    /// Contains metadata about how database columns map to entity properties and their aliases.
    /// </summary>
    public class SelectMapping
    {
        /// <summary>
        /// Gets or sets the name of the database column to select from.
        /// </summary>
        /// <value>The database column name as it appears in the SQL table schema.</value>
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the alias name to use for the column in the SQL SELECT clause.
        /// </summary>
        /// <value>The alias name that will be used in the result set, or null if no alias is needed.</value>
        public string Alias { get; set; }

        /// <summary>
        /// Gets or sets the PropertyInfo of the source entity property that maps to the database column.
        /// </summary>
        /// <value>The PropertyInfo representing the original entity property being selected from.</value>
        public PropertyInfo SourceProperty { get; set; }

        /// <summary>
        /// Gets or sets the PropertyInfo of the target property in the projection result type.
        /// </summary>
        /// <value>The PropertyInfo representing the property in the result object where the selected value will be assigned, or null for identity projections.</value>
        public PropertyInfo TargetProperty { get; set; }
    }
}