namespace Durable.Query
{
    using System;

    /// <summary>
    /// Base class of the backend-neutral query tree that <see cref="QueryNormalizer"/> produces from LINQ expressions.
    /// Every backend (SQL dialects, document stores, search engines, graph stores, in-memory) translates the same tree,
    /// so closure evaluation, enum and null handling, navigation analysis and grouping are implemented once.
    /// Nodes carry C# semantics: comparisons treat <c>null == null</c> as true, negation of a comparison with null is true,
    /// and string operations carry a <see cref="StringMatchMode"/>. Translate a tree with a <see cref="QueryNodeVisitor{TResult}"/>.
    /// Thread safety: nodes are immutable.
    /// </summary>
    public abstract class QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the CLR type of the value the node produces (<see cref="bool"/> for conditions). Never null.
        /// </summary>
        public Type ClrType { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when clrType is null.</exception>
        protected QueryNode(Type clrType)
        {
            ClrType = clrType ?? throw new ArgumentNullException(nameof(clrType));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Dispatches to the matching <c>Visit</c> method of a visitor.
        /// </summary>
        /// <typeparam name="TResult">Visitor result type.</typeparam>
        /// <param name="visitor">Visitor. Must not be null.</param>
        /// <returns>The visitor's result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when visitor is null.</exception>
        public abstract TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor);

        #endregion
    }
}
