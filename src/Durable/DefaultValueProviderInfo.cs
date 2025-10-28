namespace Durable
{
    /// <summary>
    /// Contains information about a default value provider for a property.
    /// </summary>
    public class DefaultValueProviderInfo
    {
        /// <summary>
        /// The default value attribute.
        /// </summary>
        public DefaultValueAttribute Attribute { get; set; }

        /// <summary>
        /// The default value provider implementation.
        /// </summary>
        public IDefaultValueProvider Provider { get; set; }

        /// <summary>
        /// Creates a new default value provider info.
        /// </summary>
        public DefaultValueProviderInfo(DefaultValueAttribute attribute, IDefaultValueProvider provider)
        {
            Attribute = attribute;
            Provider = provider;
        }
    }
}
