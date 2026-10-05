namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// A client-side value (constant or captured variable), already evaluated. When the value is compared with a column, <see cref="Column"/> names that column so the backend can convert the value with the column's rules (enums, converters, JSON).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ValueNode : QueryNode
    {
        #region Public-Members

        /// <summary>
        /// Gets the value; may be null.
        /// </summary>
        public object? Value { get; }

        /// <summary>
        /// Gets the column the value is compared with; null when there is none.
        /// </summary>
        public ColumnMetadata? Column { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the node.
        /// </summary>
        /// <param name="value">The value; may be null.</param>
        /// <param name="column">The column the value is compared with; null when there is none.</param>
        /// <param name="clrType">CLR type of the node's value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public ValueNode(object? value, ColumnMetadata? column, Type clrType) : base(clrType ?? throw new ArgumentNullException(nameof(clrType)))
        {
            Value = value;
            Column = column;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override TResult Accept<TResult>(QueryNodeVisitor<TResult> visitor)
        {
            ArgumentNullException.ThrowIfNull(visitor);
            return visitor.VisitValue(this);
        }

        #endregion
    }
}
