namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// The entity types the kit stores. A target that needs to declare storage up front (collections, indexes, schemas)
    /// can iterate <see cref="All"/>; storage names come from <see cref="EntityMetadata.For(Type)"/>.
    /// Thread safety: immutable.
    /// </summary>
    public static class ConformanceEntities
    {
        /// <summary>
        /// Gets every entity type used by the kit. Never null.
        /// </summary>
        public static IReadOnlyList<Type> All { get; } = new List<Type>
        {
            typeof(CfItem),
            typeof(CfOwner),
            typeof(CfPublisher),
            typeof(CfAuthor),
            typeof(CfBook),
            typeof(CfTag),
            typeof(CfAuthorTag),
            typeof(CfNote),
            typeof(CfCompositeItem),
            typeof(CfVersionedItem),
            typeof(CfUpsertItem),
            typeof(CfTenantNote),
            typeof(CfTextItem),
            typeof(CfConvertedItem),
            typeof(CfConventionWidget),
            typeof(CfFolder),
            typeof(CfDocument)
        };

        /// <summary>
        /// Gets the entity types of the author/book/publisher/tag/note relationship graph. Never null.
        /// </summary>
        public static IReadOnlyList<Type> Library { get; } = new List<Type>
        {
            typeof(CfPublisher),
            typeof(CfAuthor),
            typeof(CfBook),
            typeof(CfTag),
            typeof(CfAuthorTag),
            typeof(CfNote)
        };
    }
}
