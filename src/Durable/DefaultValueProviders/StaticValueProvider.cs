namespace Durable.DefaultValueProviders
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Provides a static value as a default value
    /// </summary>
    public class StaticValueProvider : IDefaultValueProvider
    {
        private readonly object? _value;
        private readonly bool _onlyIfNull;

        /// <summary>
        /// Initializes a new instance of the StaticValueProvider class
        /// </summary>
        /// <param name="value">The static value to provide</param>
        /// <param name="onlyIfNull">If true, only apply when the current value is null/default</param>
        public StaticValueProvider(object? value, bool onlyIfNull = true)
        {
            _value = value;
            _onlyIfNull = onlyIfNull;
        }

        /// <inheritdoc/>
        public object? GetDefaultValue(PropertyInfo property, object entity)
        {
            return _value;
        }

        /// <inheritdoc/>
        public bool ShouldApply(object? currentValue, Type propertyType)
        {
            if (!_onlyIfNull) return true;
            if (currentValue == null) return true;

            // Check if value is the default for its type
            if (propertyType.IsValueType)
            {
                object defaultValue = Activator.CreateInstance(propertyType)!;
                return currentValue.Equals(defaultValue);
            }

            return false;
        }
    }
}
