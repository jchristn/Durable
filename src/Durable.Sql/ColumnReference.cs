namespace Durable.Sql
{
    using System;
    using Durable;

    /// <summary>
    /// A mapped column together with the table source it was resolved from.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class ColumnReference
    {
        /// <summary>
        /// Gets the table source. Never null.
        /// </summary>
        public TableSource Source { get; }

        /// <summary>
        /// Gets the column. Never null.
        /// </summary>
        public ColumnMetadata Column { get; }

        /// <summary>
        /// Instantiates a column reference.
        /// </summary>
        /// <param name="source">Source. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public ColumnReference(TableSource source, ColumnMetadata column)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            Column = column ?? throw new ArgumentNullException(nameof(column));
        }
    }
}
