namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A binary arithmetic expression.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ArithmeticNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the operator.
        /// </summary>
        public ArithmeticOperator Operator { get; }

        /// <summary>
        /// Gets the left operand. Never null.
        /// </summary>
        public QueryNode Left { get; }

        /// <summary>
        /// Gets the right operand. Never null.
        /// </summary>
        public QueryNode Right { get; }

        /// <summary>
        /// Gets whether a division has integral operands in C# and therefore truncates.
        /// </summary>
        public bool IntegerDivision { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="op">The operator.</param>
        /// <param name="left">The left operand. Must not be null.</param>
        /// <param name="right">The right operand. Must not be null.</param>
        /// <param name="integerDivision">Whether a division has integral operands in C# and therefore truncates.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public ArithmeticNode(ArithmeticOperator op, QueryNode left, QueryNode right, bool integerDivision, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Operator = op;
            Left = left ?? throw new ArgumentNullException(nameof(left));
            Right = right ?? throw new ArgumentNullException(nameof(right));
            IntegerDivision = integerDivision;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitArithmetic(this);
        }

        #endregion
    }
}
