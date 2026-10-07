namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Supplies the mapping of entity types that cannot carry Durable's attributes (generated code, models owned by
    /// another team, models already annotated with another attribute system). A source answers with the same attribute
    /// objects Durable reads from classes (<see cref="EntityAttribute"/>, <see cref="PropertyAttribute"/>,
    /// <see cref="ForeignKeyAttribute"/> and so on), constructed in code, so an adapter is a translation table rather than
    /// a second mapping language.
    /// Register a source with <see cref="DurableMapping.Register{T}(IEntityMappingSource)"/> or
    /// <see cref="DurableMapping.MappingSource"/> at startup, before the first repository for a mapped type is used.
    /// Thread safety: Durable calls a source at most once per type, while building that type's cached metadata, and may
    /// do so from any thread; implementations must be safe to call concurrently for different types.
    /// </summary>
    public interface IEntityMappingSource
    {
        /// <summary>
        /// Determines whether this source supplies the mapping for a type. When false, Durable reads the type's own
        /// attributes instead. A source registered as <see cref="DurableMapping.MappingSource"/> is asked about every
        /// entity and projection type, so it must return false for types it does not know.
        /// </summary>
        /// <param name="entityType">Entity or projection type. Never null.</param>
        /// <returns>True when this source maps the type.</returns>
        bool Describes(Type entityType);

        /// <summary>
        /// Gets the table (or collection) mapping of a type this source describes.
        /// </summary>
        /// <param name="entityType">Entity type. Never null.</param>
        /// <returns>The entity attribute, or null to use the class name transformed by <see cref="DurableMapping.NamingConvention"/>.</returns>
        EntityAttribute? GetEntityAttribute(Type entityType);

        /// <summary>
        /// Gets the mapping attributes of one property of a type this source describes. Durable reads
        /// <see cref="PropertyAttribute"/>, <see cref="ForeignKeyAttribute"/>, <see cref="NavigationPropertyAttribute"/>,
        /// <see cref="InverseNavigationPropertyAttribute"/>, <see cref="ManyToManyNavigationPropertyAttribute"/>,
        /// <see cref="NotMappedAttribute"/>, <see cref="ValueConverterAttribute"/>, <see cref="VersionColumnAttribute"/>,
        /// <see cref="SoftDeleteAttribute"/>, <see cref="IndexAttribute"/> and <see cref="DefaultValueAttribute"/>; any other
        /// attribute is ignored. When no property of the type returns a <see cref="PropertyAttribute"/>, the type is mapped
        /// by convention exactly as an unannotated class is.
        /// </summary>
        /// <param name="entityType">Entity type. Never null.</param>
        /// <param name="property">Public instance property of <paramref name="entityType"/>. Never null.</param>
        /// <returns>The property's mapping attributes; null or empty when it has none.</returns>
        IEnumerable<Attribute>? GetPropertyAttributes(Type entityType, PropertyInfo property);

        /// <summary>
        /// Gets the multi-column indexes of a type this source describes.
        /// </summary>
        /// <param name="entityType">Entity type. Never null.</param>
        /// <returns>The composite indexes; null or empty when there are none.</returns>
        IEnumerable<CompositeIndexAttribute>? GetCompositeIndexes(Type entityType);
    }
}
