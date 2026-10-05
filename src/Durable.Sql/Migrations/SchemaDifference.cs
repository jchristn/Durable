namespace Durable.Sql
{
    using System;

    /// <summary>
    /// A difference between an entity mapping and the live schema that is reported but not applied automatically.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class SchemaDifference
    {
        #region Public-Members

        /// <summary>
        /// Gets the kind of difference.
        /// </summary>
        public SchemaDifferenceKind Kind { get; }

        /// <summary>
        /// Gets the table name. Never null.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets the column name; null when the difference is not about a column.
        /// </summary>
        public string? ColumnName { get; }

        /// <summary>
        /// Gets what the mapping expects (for example "varchar(100)"); null when not applicable.
        /// </summary>
        public string? Expected { get; }

        /// <summary>
        /// Gets what the database has (for example "varchar(50)"); null when not applicable.
        /// </summary>
        public string? Actual { get; }

        /// <summary>
        /// Gets a human-readable explanation including the suggested manual step. Never null.
        /// </summary>
        public string Message { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a difference.
        /// </summary>
        /// <param name="kind">Kind of difference.</param>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <param name="columnName">Column name; may be null.</param>
        /// <param name="expected">Expected value; may be null.</param>
        /// <param name="actual">Actual value; may be null.</param>
        /// <param name="message">Explanation. Must not be null.</param>
        /// <exception cref="ArgumentException">Thrown when tableName is null or empty.</exception>
        /// <exception cref="ArgumentNullException">Thrown when message is null.</exception>
        public SchemaDifference(SchemaDifferenceKind kind, string tableName, string? columnName, string? expected, string? actual, string message)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            Kind = kind;
            TableName = tableName;
            ColumnName = columnName;
            Expected = expected;
            Actual = actual;
            Message = message ?? throw new ArgumentNullException(nameof(message));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns "Kind: message".
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return Kind + ": " + Message;
        }

        #endregion
    }
}
