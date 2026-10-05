namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A member of a related entity reached through a reference navigation (<c>book.Author.Name</c>): the value of <see cref="Column"/> on the related row whose key equals <see cref="OwnerKey"/>, or null when there is none or it is soft-deleted.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class NavigationMemberNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the reference navigation. Never null.
        /// </summary>
        public NavigationMetadata Navigation { get; }

        /// <summary>
        /// Gets the owner-side key value (the navigation's local column). Never null.
        /// </summary>
        public QueryNode OwnerKey { get; }

        /// <summary>
        /// Gets the source representing the related row. Never null.
        /// </summary>
        public QuerySource RelatedSource { get; }

        /// <summary>
        /// Gets the related column read. Never null.
        /// </summary>
        public ColumnMetadata Column { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="navigation">The reference navigation. Must not be null.</param>
        /// <param name="ownerKey">The owner-side key value (the navigation's local column). Must not be null.</param>
        /// <param name="relatedSource">The source representing the related row. Must not be null.</param>
        /// <param name="column">The related column read. Must not be null.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public NavigationMemberNode(NavigationMetadata navigation, QueryNode ownerKey, QuerySource relatedSource, ColumnMetadata column, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Navigation = navigation ?? throw new ArgumentNullException(nameof(navigation));
            OwnerKey = ownerKey ?? throw new ArgumentNullException(nameof(ownerKey));
            RelatedSource = relatedSource ?? throw new ArgumentNullException(nameof(relatedSource));
            Column = column ?? throw new ArgumentNullException(nameof(column));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitNavigationMember(this);
        }

        #endregion
    }
}
