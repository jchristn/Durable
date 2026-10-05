namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A scalar function call (string, math and date functions; see <see cref="QueryFunction"/>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class FunctionNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the function.
        /// </summary>
        public QueryFunction Function { get; }

        /// <summary>
        /// Gets the arguments in the order documented on <see cref="QueryFunction"/>. Never null.
        /// </summary>
        public IReadOnlyList<QueryNode> Arguments { get; }

        /// <summary>
        /// Gets the string comparison mode for functions that compare text (Replace, IndexOf).
        /// </summary>
        public StringMatchMode StringMode { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="function">The function.</param>
        /// <param name="arguments">The arguments in the order documented on <see cref="QueryFunction"/>. Must not be null.</param>
        /// <param name="stringMode">The string comparison mode for functions that compare text (Replace, IndexOf).</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public FunctionNode(QueryFunction function, IReadOnlyList<QueryNode> arguments, StringMatchMode stringMode, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Function = function;
            Arguments = arguments ?? throw new ArgumentNullException(nameof(arguments));
            StringMode = stringMode;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitFunction(this);
        }

        #endregion
    }
}
