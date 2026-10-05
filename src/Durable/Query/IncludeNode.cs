namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// One navigation in an include tree.
    /// Thread safety: not thread-safe; owned by a query builder.
    /// </summary>
    public sealed class IncludeNode
    {
        /// <summary>
        /// Gets the navigation to load. Never null.
        /// </summary>
        public NavigationMetadata Navigation { get; }

        /// <summary>
        /// Gets nested includes loaded on the related entities. Never null.
        /// </summary>
        public List<IncludeNode> Children { get; } = new List<IncludeNode>();

        /// <summary>
        /// Instantiates a node.
        /// </summary>
        /// <param name="navigation">Navigation. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when navigation is null.</exception>
        public IncludeNode(NavigationMetadata navigation)
        {
            Navigation = navigation ?? throw new ArgumentNullException(nameof(navigation));
        }

        /// <summary>
        /// Returns the existing child for a navigation or adds a new one.
        /// </summary>
        /// <param name="navigation">Navigation. Must not be null.</param>
        /// <returns>The child node.</returns>
        public IncludeNode GetOrAddChild(NavigationMetadata navigation)
        {
            ArgumentNullException.ThrowIfNull(navigation);
            foreach (IncludeNode child in Children)
            {
                if (ReferenceEquals(child.Navigation, navigation)) return child;
            }

            IncludeNode node = new IncludeNode(navigation);
            Children.Add(node);
            return node;
        }
    }
}
