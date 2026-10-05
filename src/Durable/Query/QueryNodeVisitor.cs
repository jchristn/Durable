namespace Durable.Query
{
    using System;

    /// <summary>
    /// Base class for translating a <see cref="QueryNode"/> tree into a backend's representation (SQL text, a document
    /// filter, a predicate delegate, ...). Override the <c>Visit</c> methods for the nodes the backend supports; the
    /// defaults throw <see cref="NotSupportedException"/> naming the node, so unsupported constructs fail clearly.
    /// Thread safety: depends on the implementation; the base class holds no state.
    /// </summary>
    /// <typeparam name="TResult">Translation result type.</typeparam>
    public abstract class QueryNodeVisitor<TResult>
    {
        #region Public-Methods

        /// <summary>
        /// Translates a node by dispatching to its <c>Visit</c> method.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when node is null.</exception>
        public TResult Visit(QueryNode node)
        {
            ArgumentNullException.ThrowIfNull(node);
            return node.Accept(this);
        }

        /// <summary>
        /// Translates a <see cref="ColumnNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitColumn(ColumnNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="ValueNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitValue(ValueNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="ComparisonNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitComparison(ComparisonNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="LogicalNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitLogical(LogicalNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="NotNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitNot(NotNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="NullCheckNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitNullCheck(NullCheckNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="InNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitIn(InNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="StringMatchNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitStringMatch(StringMatchNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="StringTestNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitStringTest(StringTestNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="ArithmeticNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitArithmetic(ArithmeticNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="NegateNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitNegate(NegateNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="ConcatNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitConcat(ConcatNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="CoalesceNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitCoalesce(CoalesceNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="ConditionalNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitConditional(ConditionalNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="FunctionNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitFunction(FunctionNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="NavigationMemberNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitNavigationMember(NavigationMemberNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="CollectionNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitCollection(CollectionNode node)
        {
            throw Unsupported(node);
        }

        /// <summary>
        /// Translates a <see cref="AggregateNode"/>. The default throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="node">Node. Never null.</param>
        /// <returns>The translation.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend does not support the node.</exception>
        public virtual TResult VisitAggregate(AggregateNode node)
        {
            throw Unsupported(node);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Creates the exception thrown for a node the backend cannot translate.
        /// </summary>
        /// <param name="node">Node. Must not be null.</param>
        /// <returns>The exception.</returns>
        protected virtual NotSupportedException Unsupported(QueryNode node)
        {
            return new NotSupportedException(GetType().Name + " cannot translate " + node.GetType().Name + " (" + node.ClrType.Name + ").");
        }

        #endregion
    }
}
