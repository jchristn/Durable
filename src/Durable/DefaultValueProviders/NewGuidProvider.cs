namespace Durable.DefaultValueProviders
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Provides a new GUID as a default value
    /// </summary>
    public class NewGuidProvider : IDefaultValueProvider
    {
        /// <inheritdoc/>
        public object? GetDefaultValue(PropertyInfo property, object entity)
        {
            return Guid.NewGuid();
        }

        /// <inheritdoc/>
        public bool ShouldApply(object? currentValue, Type propertyType)
        {
            if (currentValue == null) return true;

            if (propertyType == typeof(Guid) || propertyType == typeof(Guid?))
            {
                Guid guid = (currentValue is Guid g) ? g : default;
                return guid == default;
            }

            return false;
        }
    }
}
