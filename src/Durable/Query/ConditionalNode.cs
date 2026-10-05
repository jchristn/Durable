namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A conditional value (<c>test ? a : b</c>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ConditionalNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the condition. Never null.
        /// </summary>
        public QueryNode Test { get; }

        /// <summary>
        /// Gets the value when the condition holds. Never null.
        /// </summary>
        public QueryNode IfTrue { get; }

        /// <summary>
        /// Gets the value otherwise. Never null.
        /// </summary>
        public QueryNode IfFalse { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="test">The condition. Must not be null.</param>
        /// <param name="ifTrue">The value when the condition holds. Must not be null.</param>
        /// <param name="ifFalse">The value otherwise. Must not be null.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public ConditionalNode(QueryNode test, QueryNode ifTrue, QueryNode ifFalse, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Test = test ?? throw new ArgumentNullException(nameof(test));
            IfTrue = ifTrue ?? throw new ArgumentNullException(nameof(ifTrue));
            IfFalse = ifFalse ?? throw new ArgumentNullException(nameof(ifFalse));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitConditional(this);
        }

        #endregion
    }
}
