namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// An aggregate over the rows of the current group (<c>g.Count()</c>, <c>g.Sum(x =&gt; x.Price)</c>, <c>g.Any(x =&gt; ...)</c>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class AggregateNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the aggregate.
        /// </summary>
        public AggregateFunction Function { get; }

        /// <summary>
        /// Gets the aggregated value for Sum/Average/Min/Max, or the row condition for Count/Any; null for an unconditional Count/Any.
        /// </summary>
        public QueryNode? Operand { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="function">The aggregate.</param>
        /// <param name="operand">The aggregated value for Sum/Average/Min/Max, or the row condition for Count/Any; null for an unconditional Count/Any.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public AggregateNode(AggregateFunction function, QueryNode? operand, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Function = function;
            Operand = operand;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitAggregate(this);
        }

        #endregion
    }
}
