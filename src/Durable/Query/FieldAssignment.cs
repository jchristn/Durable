namespace Durable.Query
{
    using System;
    using Durable;

    /// <summary>
    /// One column assignment of a set-based update (<c>UpdateField</c>, <c>BatchUpdate</c>, version bumps).
    /// The value is a node over the model's source, so it may refer to the row's current values (<c>x.Count + 1</c>).
    /// Thread safety: immutable.
    /// </summary>
    public sealed class FieldAssignment
    {
        #region Public-Members

        /// <summary>
        /// Gets the assigned column. Never null.
        /// </summary>
        public ColumnMetadata Column { get; }

        /// <summary>
        /// Gets the new value. Never null.
        /// </summary>
        public QueryNode Value { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an assignment.
        /// </summary>
        /// <param name="column">Assigned column. Must not be null.</param>
        /// <param name="value">New value. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public FieldAssignment(ColumnMetadata column, QueryNode value)
        {
            Column = column ?? throw new ArgumentNullException(nameof(column));
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        #endregion
    }
}
