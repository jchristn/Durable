namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A boolean AND or OR of two conditions.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class LogicalNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the operator.
        /// </summary>
        public LogicalOperator Operator { get; }

        /// <summary>
        /// Gets the left condition. Never null.
        /// </summary>
        public QueryNode Left { get; }

        /// <summary>
        /// Gets the right condition. Never null.
        /// </summary>
        public QueryNode Right { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="op">The operator.</param>
        /// <param name="left">The left condition. Must not be null.</param>
        /// <param name="right">The right condition. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public LogicalNode(LogicalOperator op, QueryNode left, QueryNode right) : base(typeof(bool))
        {
            Operator = op;
            Left = left ?? throw new ArgumentNullException(nameof(left));
            Right = right ?? throw new ArgumentNullException(nameof(right));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitLogical(this);
        }

        #endregion
    }
}
