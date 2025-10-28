namespace Durable.DefaultValueProviders
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Provides the current UTC date and time as a default value
    /// </summary>
    public class CurrentDateTimeUtcProvider : IDefaultValueProvider
    {
        /// <inheritdoc/>
        public object? GetDefaultValue(PropertyInfo property, object entity)
        {
            return DateTime.UtcNow;
        }

        /// <inheritdoc/>
        public bool ShouldApply(object? currentValue, Type propertyType)
        {
            if (currentValue == null) return true;

            if (propertyType == typeof(DateTime) || propertyType == typeof(DateTime?))
            {
                DateTime dt = (currentValue is DateTime dateTime) ? dateTime : default;
                return dt == default;
            }

            return false;
        }
    }
}
