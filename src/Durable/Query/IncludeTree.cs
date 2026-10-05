namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Durable;

    /// <summary>
    /// Builds the include tree for a query from <c>Include</c> and <c>ThenInclude</c> lambdas
    /// (<c>x =&gt; x.Author</c>, <c>x =&gt; x.Author.Company</c>). Repeated paths share nodes.
    /// Thread safety: not thread-safe; owned by a query builder.
    /// </summary>
    public sealed class IncludeTree
    {
        #region Public-Members

        /// <summary>
        /// Gets the root entity metadata. Never null.
        /// </summary>
        public EntityMetadata Root { get; }

        /// <summary>
        /// Gets the top-level includes. Never null.
        /// </summary>
        public IReadOnlyList<IncludeNode> Nodes => _Nodes;

        /// <summary>
        /// Gets whether any include was added.
        /// </summary>
        public bool IsEmpty => _Nodes.Count == 0;

        #endregion

        #region Private-Members

        private readonly List<IncludeNode> _Nodes = new List<IncludeNode>();
        private IncludeNode? _Last;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an empty include tree.
        /// </summary>
        /// <param name="root">Root entity metadata. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when root is null.</exception>
        public IncludeTree(EntityMetadata root)
        {
            Root = root ?? throw new ArgumentNullException(nameof(root));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Adds a navigation path from the root.
        /// </summary>
        /// <param name="navigationProperty">Navigation member access over the root entity. Must not be null.</param>
        /// <returns>The deepest node of the path.</returns>
        /// <exception cref="ArgumentNullException">Thrown when navigationProperty is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the lambda is not a navigation member chain.</exception>
        public IncludeNode Include(LambdaExpression navigationProperty)
        {
            ArgumentNullException.ThrowIfNull(navigationProperty);
            EntityMetadata owner = Root;
            IncludeNode? node = null;
            foreach (string name in NavigationPath(navigationProperty.Body))
            {
                NavigationMetadata navigation = owner.FindNavigation(name)
                    ?? throw new ArgumentException("'" + name + "' is not a navigation property of " + owner.EntityType.Name + ".", nameof(navigationProperty));
                node = node == null ? GetOrAddRoot(navigation) : node.GetOrAddChild(navigation);
                owner = EntityMetadata.For(navigation.RelatedType);
            }

            _Last = node;
            return node!;
        }

        /// <summary>
        /// Adds a navigation path below the most recently included navigation.
        /// </summary>
        /// <param name="previousType">Entity type of the previous include (the lambda's parameter type). Must not be null.</param>
        /// <param name="navigationProperty">Navigation member access over the previous include's entity. Must not be null.</param>
        /// <returns>The deepest node of the path.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when no include precedes it or the types do not match.</exception>
        /// <exception cref="ArgumentException">Thrown when the lambda is not a navigation member chain.</exception>
        public IncludeNode ThenInclude(Type previousType, LambdaExpression navigationProperty)
        {
            ArgumentNullException.ThrowIfNull(previousType);
            ArgumentNullException.ThrowIfNull(navigationProperty);
            if (_Last == null) throw new InvalidOperationException("ThenInclude must follow Include.");
            if (_Last.Navigation.RelatedType != previousType)
                throw new InvalidOperationException("ThenInclude expects the previous include's entity type " + _Last.Navigation.RelatedType.Name + " but received " + previousType.Name + ".");

            EntityMetadata owner = EntityMetadata.For(previousType);
            IncludeNode node = _Last;
            foreach (string name in NavigationPath(navigationProperty.Body))
            {
                NavigationMetadata navigation = owner.FindNavigation(name)
                    ?? throw new ArgumentException("'" + name + "' is not a navigation property of " + owner.EntityType.Name + ".", nameof(navigationProperty));
                node = node.GetOrAddChild(navigation);
                owner = EntityMetadata.For(navigation.RelatedType);
            }

            _Last = node;
            return node;
        }

        /// <summary>
        /// Enumerates every node in the tree, depth first.
        /// </summary>
        /// <returns>The nodes.</returns>
        public IEnumerable<IncludeNode> All()
        {
            Stack<IncludeNode> pending = new Stack<IncludeNode>();
            for (int i = _Nodes.Count - 1; i >= 0; i--) pending.Push(_Nodes[i]);
            while (pending.Count > 0)
            {
                IncludeNode node = pending.Pop();
                yield return node;
                for (int i = node.Children.Count - 1; i >= 0; i--) pending.Push(node.Children[i]);
            }
        }

        #endregion

        #region Private-Methods

        private IncludeNode GetOrAddRoot(NavigationMetadata navigation)
        {
            foreach (IncludeNode node in _Nodes)
            {
                if (ReferenceEquals(node.Navigation, navigation)) return node;
            }

            IncludeNode added = new IncludeNode(navigation);
            _Nodes.Add(added);
            return added;
        }

        private static List<string> NavigationPath(Expression body)
        {
            List<string> path = new List<string>();
            Expression current = body;
            while (current is UnaryExpression unary && (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.TypeAs)) current = unary.Operand;
            while (current is MemberExpression member)
            {
                path.Insert(0, member.Member.Name);
                current = member.Expression!;
            }

            if (current is not ParameterExpression || path.Count == 0)
                throw new ArgumentException("Include expects a navigation member access such as x => x.Author or x => x.Author.Company.");
            return path;
        }

        #endregion
    }
}
