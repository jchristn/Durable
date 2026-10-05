namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A comparison with C# semantics: <c>null == null</c> is true and <c>null != value</c> is true. Comparisons with a null constant are normalized to <see cref="NullCheckNode"/>. For string operands, <see cref="StringMode"/> says how to compare.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ComparisonNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the operator.
        /// </summary>
        public ComparisonOperator Operator { get; }

        /// <summary>
        /// Gets the left operand. Never null.
        /// </summary>
        public QueryNode Left { get; }

        /// <summary>
        /// Gets the right operand. Never null.
        /// </summary>
        public QueryNode Right { get; }

        /// <summary>
        /// Gets the string comparison mode; meaningful only for string operands.
        /// </summary>
        public StringMatchMode StringMode { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="op">The operator.</param>
        /// <param name="left">The left operand. Must not be null.</param>
        /// <param name="right">The right operand. Must not be null.</param>
        /// <param name="stringMode">The string comparison mode; meaningful only for string operands.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public ComparisonNode(ComparisonOperator op, QueryNode left, QueryNode right, StringMatchMode stringMode) : base(typeof(bool))
        {
            Operator = op;
            Left = left ?? throw new ArgumentNullException(nameof(left));
            Right = right ?? throw new ArgumentNullException(nameof(right));
            StringMode = stringMode;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitComparison(this);
        }

        #endregion
    }
}
