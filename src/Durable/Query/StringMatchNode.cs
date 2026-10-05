namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A substring test (Contains, StartsWith, EndsWith). The pattern is literal text, not a wildcard pattern; backends escape it as needed.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class StringMatchNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the kind of test.
        /// </summary>
        public StringMatchKind Kind { get; }

        /// <summary>
        /// Gets the searched string. Never null.
        /// </summary>
        public QueryNode Target { get; }

        /// <summary>
        /// Gets the text to find. Never null.
        /// </summary>
        public QueryNode Pattern { get; }

        /// <summary>
        /// Gets how characters are compared.
        /// </summary>
        public StringMatchMode Mode { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="kind">The kind of test.</param>
        /// <param name="target">The searched string. Must not be null.</param>
        /// <param name="pattern">The text to find. Must not be null.</param>
        /// <param name="mode">How characters are compared.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public StringMatchNode(StringMatchKind kind, QueryNode target, QueryNode pattern, StringMatchMode mode) : base(typeof(bool))
        {
            Kind = kind;
            Target = target ?? throw new ArgumentNullException(nameof(target));
            Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
            Mode = mode;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitStringMatch(this);
        }

        #endregion
    }
}
