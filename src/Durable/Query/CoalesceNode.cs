namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// The left value, or the right value when the left is null (<c>??</c>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class CoalesceNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the preferred value. Never null.
        /// </summary>
        public QueryNode Left { get; }

        /// <summary>
        /// Gets the fallback value. Never null.
        /// </summary>
        public QueryNode Right { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="left">The preferred value. Must not be null.</param>
        /// <param name="right">The fallback value. Must not be null.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public CoalesceNode(QueryNode left, QueryNode right, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Left = left ?? throw new ArgumentNullException(nameof(left));
            Right = right ?? throw new ArgumentNullException(nameof(right));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitCoalesce(this);
        }

        #endregion
    }
}
