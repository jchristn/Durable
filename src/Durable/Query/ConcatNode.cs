namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// String concatenation with C# semantics: null parts become empty strings and non-string parts are converted to text.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ConcatNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the parts in order. Never null.
        /// </summary>
        public IReadOnlyList<QueryNode> Parts { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="parts">The parts in order. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public ConcatNode(IReadOnlyList<QueryNode> parts) : base(typeof(string))
        {
            Parts = parts ?? throw new ArgumentNullException(nameof(parts));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitConcat(this);
        }

        #endregion
    }
}
