namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Maps database columns to entity properties for projection queries in PostgreSQL.
    /// Handles SELECT clause mapping and data type conversions.
    /// </summary>
    internal class PostgresSelectMapping
    {

        #region Public-Members

        /// <summary>
        /// Gets or sets the target property to map the column value to.
        /// </summary>
        public PropertyInfo TargetProperty { get; set; } = null!;

        /// <summary>
        /// Gets or sets the source column name in the database result set.
        /// </summary>
        public string SourceColumn { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the target property name on the result object.
        /// </summary>
        public string TargetPropertyName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether this mapping requires type conversion.
        /// </summary>
        public bool RequiresConversion { get; set; } = false;

        /// <summary>
        /// Gets or sets the expected source type from the database.
        /// </summary>
        public Type? SourceType { get; set; }

        /// <summary>
        /// Gets or sets the target type for the property.
        /// </summary>
        public Type? TargetType { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresSelectMapping.
        /// </summary>
        public PostgresSelectMapping()
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresSelectMapping with the specified property and column.
        /// </summary>
        /// <param name="targetProperty">The target property to map to</param>
        /// <param name="sourceColumn">The source column name</param>
        /// <exception cref="ArgumentNullException">Thrown when targetProperty is null</exception>
        /// <exception cref="ArgumentException">Thrown when sourceColumn is null or empty</exception>
        public PostgresSelectMapping(PropertyInfo targetProperty, string sourceColumn)
        {
            TargetProperty = targetProperty ?? throw new ArgumentNullException(nameof(targetProperty));
            if (string.IsNullOrWhiteSpace(sourceColumn))
                throw new ArgumentException("Source column cannot be null or empty", nameof(sourceColumn));

            SourceColumn = sourceColumn;
            TargetPropertyName = targetProperty.Name;
            TargetType = targetProperty.PropertyType;

            // Determine if conversion will be needed (this is a simplified check)
            RequiresConversion = IsConversionRequired(targetProperty.PropertyType);
        }

        /// <summary>
        /// Creates a collection of select mappings from a dictionary of column mappings.
        /// </summary>
        /// <param name="columnMappings">Dictionary mapping column names to properties</param>
        /// <returns>A collection of PostgresSelectMapping objects</returns>
        /// <exception cref="ArgumentNullException">Thrown when columnMappings is null</exception>
        public static IEnumerable<PostgresSelectMapping> FromColumnMappings(Dictionary<string, PropertyInfo> columnMappings)
        {
            if (columnMappings == null)
                throw new ArgumentNullException(nameof(columnMappings));

            List<PostgresSelectMapping> mappings = new List<PostgresSelectMapping>();

            foreach (KeyValuePair<string, PropertyInfo> kvp in columnMappings)
            {
                mappings.Add(new PostgresSelectMapping(kvp.Value, kvp.Key));
            }

            return mappings;
        }

        /// <summary>
        /// Creates select mappings for a specific result type.
        /// </summary>
        /// <typeparam name="TResult">The result type to create mappings for</typeparam>
        /// <returns>A collection of PostgresSelectMapping objects</returns>
        public static IEnumerable<PostgresSelectMapping> ForResultType<TResult>() where TResult : new()
        {
            List<PostgresSelectMapping> mappings = new List<PostgresSelectMapping>();
            Type resultType = typeof(TResult);

            foreach (PropertyInfo property in resultType.GetProperties())
            {
                if (property.CanWrite)
                {
                    // Use property name as column name by default
                    string columnName = property.Name;
                    mappings.Add(new PostgresSelectMapping(property, columnName));
                }
            }

            return mappings;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Applies this mapping to set a property value from a database column value.
        /// </summary>
        /// <param name="target">The target object to set the property on</param>
        /// <param name="columnValue">The value from the database column</param>
        /// <param name="converter">Optional data type converter for type conversions</param>
        /// <exception cref="ArgumentNullException">Thrown when target is null</exception>
        public void ApplyMapping(object target, object? columnValue, IDataTypeConverter? converter = null)
        {
            if (target == null)
                throw new ArgumentNullException(nameof(target));

            if (!TargetProperty.CanWrite)
                return;

            try
            {
                object? convertedValue = columnValue;

                // Handle null values
                if (columnValue == null || columnValue == DBNull.Value)
                {
                    if (TargetType != null && Nullable.GetUnderlyingType(TargetType) == null && TargetType.IsValueType)
                    {
                        // Non-nullable value type, use default value
                        convertedValue = Activator.CreateInstance(TargetType);
                    }
                    else
                    {
                        convertedValue = null;
                    }
                }
                else if (RequiresConversion && converter != null && TargetType != null)
                {
                    // Use converter for type conversion
                    convertedValue = converter.ConvertFromDatabase(columnValue, TargetType, TargetProperty);
                }
                else if (RequiresConversion)
                {
                    // Basic type conversion without converter
                    convertedValue = Convert.ChangeType(columnValue, TargetType ?? typeof(object));
                }

                TargetProperty.SetValue(target, convertedValue);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error applying mapping for property '{TargetPropertyName}' from column '{SourceColumn}': {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Returns a string representation of this mapping.
        /// </summary>
        /// <returns>A string describing the mapping</returns>
        public override string ToString()
        {
            return $"'{SourceColumn}' -> '{TargetPropertyName}' ({TargetType?.Name ?? "Unknown"})";
        }

        #endregion

        #region Private-Methods

        private static bool IsConversionRequired(Type propertyType)
        {
            // Simple heuristic - assume conversion is needed for non-basic types
            Type underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

            return !IsPrimitiveType(underlyingType);
        }

        private static bool IsPrimitiveType(Type type)
        {
            return type.IsPrimitive ||
                   type == typeof(string) ||
                   type == typeof(decimal) ||
                   type == typeof(DateTime) ||
                   type == typeof(TimeSpan) ||
                   type == typeof(DateTimeOffset) ||
                   type == typeof(Guid);
        }

        #endregion
    }
}