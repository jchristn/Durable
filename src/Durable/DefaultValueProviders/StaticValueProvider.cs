namespace Durable.DefaultValueProviders
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Provides a static value as a default value
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public class StaticValueProvider : IDefaultValueProvider
    {
        private readonly object? _Value;
        private readonly bool _OnlyIfNull;

        /// <summary>
        /// Initializes a new instance of the StaticValueProvider class
        /// </summary>
        /// <param name="value">The static value to provide</param>
        /// <param name="onlyIfNull">If true, only apply when the current value is null/default</param>
        public StaticValueProvider(object? value, bool onlyIfNull = true)
        {
            _Value = value;
            _OnlyIfNull = onlyIfNull;
        }

        /// <inheritdoc/>
        public object? GetDefaultValue(PropertyInfo property, object entity)
        {
            return _Value;
        }

        /// <inheritdoc/>
        public bool ShouldApply(object? currentValue, Type propertyType)
        {
            if (!_OnlyIfNull) return true;
            if (currentValue == null) return true;

            // Check if value is the default for its type
            if (propertyType.IsValueType)
            {
                object defaultValue = MemberAccessorFactory.GetDefaultValue(propertyType)!;
                return currentValue.Equals(defaultValue);
            }

            return false;
        }
    }
}
