namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A null test (<c>x == null</c>, <c>x != null</c>, <c>HasValue</c>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class NullCheckNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the tested value. Never null.
        /// </summary>
        public QueryNode Operand { get; }

        /// <summary>
        /// Gets whether the test is for null (true) or for a value (false).
        /// </summary>
        public bool IsNull { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="operand">The tested value. Must not be null.</param>
        /// <param name="isNull">Whether the test is for null (true) or for a value (false).</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public NullCheckNode(QueryNode operand, bool isNull) : base(typeof(bool))
        {
            Operand = operand ?? throw new ArgumentNullException(nameof(operand));
            IsNull = isNull;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitNullCheck(this);
        }

        #endregion
    }
}
