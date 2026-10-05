namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A unary string test (<c>string.IsNullOrEmpty</c>, <c>string.IsNullOrWhiteSpace</c>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class StringTestNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the kind of test.
        /// </summary>
        public StringTestKind Kind { get; }

        /// <summary>
        /// Gets the tested string. Never null.
        /// </summary>
        public QueryNode Operand { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="kind">The kind of test.</param>
        /// <param name="operand">The tested string. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public StringTestNode(StringTestKind kind, QueryNode operand) : base(typeof(bool))
        {
            Kind = kind;
            Operand = operand ?? throw new ArgumentNullException(nameof(operand));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitStringTest(this);
        }

        #endregion
    }
}
