namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Boolean negation with C# semantics (a comparison involving null is false, so its negation is true).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class NotNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the negated condition. Never null.
        /// </summary>
        public QueryNode Operand { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="operand">The negated condition. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public NotNode(QueryNode operand) : base(typeof(bool))
        {
            Operand = operand ?? throw new ArgumentNullException(nameof(operand));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitNot(this);
        }

        #endregion
    }
}
