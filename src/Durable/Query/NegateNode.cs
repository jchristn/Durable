namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Arithmetic negation.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class NegateNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the negated value. Never null.
        /// </summary>
        public QueryNode Operand { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="operand">The negated value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public NegateNode(QueryNode operand) : base((operand ?? throw new ArgumentNullException(nameof(operand))).ClrType)
        {
            Operand = operand ?? throw new ArgumentNullException(nameof(operand));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitNegate(this);
        }

        #endregion
    }
}
