namespace Durable
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Provides type conversion functionality between .NET types and database storage formats.
    /// </summary>
    public interface IDataTypeConverter
    {
        /// <summary>
        /// Converts a .NET object to its database storage representation.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <param name="targetType">The target database type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based conversion hints.</param>
        /// <returns>The database-compatible representation of the value.</returns>
        object ConvertToDatabase(object value, Type targetType, PropertyInfo? propertyInfo = null);
        
        /// <summary>
        /// Converts a database value to its .NET type representation.
        /// </summary>
        /// <param name="value">The database value to convert.</param>
        /// <param name="targetType">The target .NET type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based conversion hints.</param>
        /// <returns>The .NET object representation of the database value.</returns>
        object? ConvertFromDatabase(object? value, Type targetType, PropertyInfo? propertyInfo = null);
        
        /// <summary>
        /// Determines whether the converter can handle the specified type.
        /// </summary>
        /// <param name="type">The type to check for conversion support.</param>
        /// <returns>True if the type can be converted; otherwise, false.</returns>
        bool CanConvert(Type type);
        
        /// <summary>
        /// Gets the appropriate database type string for the specified .NET type.
        /// </summary>
        /// <param name="type">The .NET type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based type mapping hints.</param>
        /// <returns>The database type string (e.g., "TEXT", "INTEGER", "REAL").</returns>
        string GetDatabaseTypeString(Type type, PropertyInfo? propertyInfo = null);
    }
}