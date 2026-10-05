namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// An operation over the related rows of a collection or many-to-many navigation (<c>author.Books.Any(b =&gt; ...)</c>, <c>All</c>, <c>Count</c>). Soft-deleted related rows are excluded.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class CollectionNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the operation.
        /// </summary>
        public CollectionOperation Operation { get; }

        /// <summary>
        /// Gets the collection navigation. Never null.
        /// </summary>
        public NavigationMetadata Navigation { get; }

        /// <summary>
        /// Gets the owner-side key value. Never null.
        /// </summary>
        public QueryNode OwnerKey { get; }

        /// <summary>
        /// Gets the source representing each related row; <see cref="Predicate"/> refers to it. Never null.
        /// </summary>
        public QuerySource RelatedSource { get; }

        /// <summary>
        /// Gets the condition on related rows; null when there is none.
        /// </summary>
        public QueryNode? Predicate { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="navigation">The collection navigation. Must not be null.</param>
        /// <param name="ownerKey">The owner-side key value. Must not be null.</param>
        /// <param name="relatedSource">The source representing each related row; <see cref="Predicate"/> refers to it. Must not be null.</param>
        /// <param name="predicate">The condition on related rows; null when there is none.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public CollectionNode(CollectionOperation operation, NavigationMetadata navigation, QueryNode ownerKey, QuerySource relatedSource, QueryNode? predicate, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Operation = operation;
            Navigation = navigation ?? throw new ArgumentNullException(nameof(navigation));
            OwnerKey = ownerKey ?? throw new ArgumentNullException(nameof(ownerKey));
            RelatedSource = relatedSource ?? throw new ArgumentNullException(nameof(relatedSource));
            Predicate = predicate;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitCollection(this);
        }

        #endregion
    }
}
