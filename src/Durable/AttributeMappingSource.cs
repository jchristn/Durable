namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// The built-in mapping source: reads Durable's attributes from the entity class itself.
    /// Exposed as <see cref="DurableMapping.AttributeSource"/> so adapters can combine it with their own translations.
    /// Thread safety: stateless; safe to call concurrently.
    /// </summary>
    internal sealed class AttributeMappingSource : IEntityMappingSource
    {
        #region Public-Members

        /// <summary>
        /// The shared instance.
        /// </summary>
        public static readonly AttributeMappingSource Instance = new AttributeMappingSource();

        #endregion

        #region Constructors-and-Factories

        private AttributeMappingSource()
        {
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public bool Describes(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return true;
        }

        /// <inheritdoc />
        public EntityAttribute? GetEntityAttribute(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return entityType.GetCustomAttribute<EntityAttribute>();
        }

        /// <inheritdoc />
        public IEnumerable<Attribute> GetPropertyAttributes(Type entityType, PropertyInfo property)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(property);
            return Attribute.GetCustomAttributes(property, true);
        }

        /// <inheritdoc />
        public IEnumerable<CompositeIndexAttribute> GetCompositeIndexes(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return entityType.GetCustomAttributes<CompositeIndexAttribute>(true);
        }

        #endregion
    }
}
