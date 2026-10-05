namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Membership of a value in a client-side list (<c>list.Contains(x.Prop)</c>, <c>x.Prop.In(...)</c>). <see cref="Values"/> may contain null and duplicates; C# semantics apply (null matches a null element).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class InNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the tested value. Never null.
        /// </summary>
        public QueryNode Item { get; }

        /// <summary>
        /// Gets the candidate values, already evaluated (integers for enum columns are converted to the enum). Never null.
        /// </summary>
        public IReadOnlyList<object?> Values { get; }

        /// <summary>
        /// Gets whether the test is negated (<c>NotIn</c>).
        /// </summary>
        public bool Negated { get; }

        /// <summary>
        /// Gets the string comparison mode; meaningful only for string items.
        /// </summary>
        public StringMatchMode StringMode { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="item">The tested value. Must not be null.</param>
        /// <param name="values">The candidate values, already evaluated (integers for enum columns are converted to the enum). Must not be null.</param>
        /// <param name="negated">Whether the test is negated (<c>NotIn</c>).</param>
        /// <param name="stringMode">The string comparison mode; meaningful only for string items.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public InNode(QueryNode item, IReadOnlyList<object?> values, bool negated, StringMatchMode stringMode) : base(typeof(bool))
        {
            Item = item ?? throw new ArgumentNullException(nameof(item));
            Values = values ?? throw new ArgumentNullException(nameof(values));
            Negated = negated;
            StringMode = stringMode;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitIn(this);
        }

        #endregion
    }
}
