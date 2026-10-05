namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A mapped column of an entity bound to a <see cref="QuerySource"/>.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ColumnNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the source (entity instance) the column belongs to. Never null.
        /// </summary>
        public QuerySource Source { get; }

        /// <summary>
        /// Gets the column. Never null.
        /// </summary>
        public ColumnMetadata Column { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="source">The source (entity instance) the column belongs to. Must not be null.</param>
        /// <param name="column">The column. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public ColumnNode(QuerySource source, ColumnMetadata column) : base((column ?? throw new ArgumentNullException(nameof(column))).PropertyType)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            Column = column ?? throw new ArgumentNullException(nameof(column));
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitColumn(this);
        }

        #endregion
    }
}
