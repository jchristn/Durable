namespace Durable
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Interface for generating default values for entity properties during Create operations
    /// </summary>
    public interface IDefaultValueProvider
    {
        /// <summary>
        /// Gets a default value for the specified property on the given entity
        /// </summary>
        /// <param name="property">The property for which to generate a default value</param>
        /// <param name="entity">The entity instance being created</param>
        /// <returns>The default value to assign to the property</returns>
        object? GetDefaultValue(PropertyInfo property, object entity);

        /// <summary>
        /// Determines whether the default value should be applied based on the current property value
        /// </summary>
        /// <param name="currentValue">The current value of the property</param>
        /// <param name="propertyType">The type of the property</param>
        /// <returns>True if the default should be applied; otherwise, false</returns>
        bool ShouldApply(object? currentValue, Type propertyType);
    }
}
