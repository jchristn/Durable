namespace Durable
{
    using System;

    /// <summary>
    /// Attribute to specify a default value for a property when creating new entities.
    /// The default value is applied during Create operations if the property has not been explicitly set.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class DefaultValueAttribute : Attribute
    {
        /// <summary>
        /// Gets the type of built-in default value to use (if applicable)
        /// </summary>
        public DefaultValueType ValueType { get; }

        /// <summary>
        /// Gets the static value to use as default (if applicable)
        /// </summary>
        public object? StaticValue { get; }

        /// <summary>
        /// Gets the custom provider type for generating default values (if applicable)
        /// </summary>
        public Type? ProviderType { get; }

        /// <summary>
        /// Gets whether to apply the default value only when the property is null/default
        /// </summary>
        public bool OnlyIfNull { get; }

        /// <summary>
        /// Initializes a new instance of the DefaultValueAttribute class with a built-in value type
        /// </summary>
        /// <param name="valueType">The type of default value to generate</param>
        /// <param name="onlyIfNull">If true, only apply default when property is null/default. Default is true.</param>
        public DefaultValueAttribute(DefaultValueType valueType, bool onlyIfNull = true)
        {
            ValueType = valueType;
            OnlyIfNull = onlyIfNull;
        }

        /// <summary>
        /// Initializes a new instance of the DefaultValueAttribute class with a static value
        /// </summary>
        /// <param name="staticValue">The static value to use as default</param>
        /// <param name="onlyIfNull">If true, only apply default when property is null/default. Default is true.</param>
        public DefaultValueAttribute(object staticValue, bool onlyIfNull = true)
        {
            ValueType = DefaultValueType.StaticValue;
            StaticValue = staticValue;
            OnlyIfNull = onlyIfNull;
        }

        /// <summary>
        /// Initializes a new instance of the DefaultValueAttribute class with a custom provider type
        /// </summary>
        /// <param name="providerType">The type implementing IDefaultValueProvider to generate default values</param>
        /// <param name="onlyIfNull">If true, only apply default when property is null/default. Default is true.</param>
        public DefaultValueAttribute(Type providerType, bool onlyIfNull = true)
        {
            if (providerType == null)
                throw new ArgumentNullException(nameof(providerType));

            if (!typeof(IDefaultValueProvider).IsAssignableFrom(providerType))
                throw new ArgumentException($"Provider type must implement IDefaultValueProvider", nameof(providerType));

            ValueType = DefaultValueType.CustomProvider;
            ProviderType = providerType;
            OnlyIfNull = onlyIfNull;
        }
    }
}
