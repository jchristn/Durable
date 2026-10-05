namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// One schema change with the SQL statements (rendered by the dialect) that perform it.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class MigrationOperation
    {
        #region Public-Members

        /// <summary>
        /// Gets the kind of change.
        /// </summary>
        public MigrationOperationKind Kind { get; }

        /// <summary>
        /// Gets the affected table name. Never null.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets the affected column name for column operations; null otherwise.
        /// </summary>
        public string? ColumnName { get; }

        /// <summary>
        /// Gets the affected index name for index operations; null otherwise.
        /// </summary>
        public string? IndexName { get; }

        /// <summary>
        /// Gets whether the operation can lose data or break existing readers (drops, or re-creating a changed index).
        /// Destructive operations are applied only when <see cref="SchemaSyncOptions.AllowDestructive"/> is true.
        /// </summary>
        public bool IsDestructive { get; }

        /// <summary>
        /// Gets a human-readable description. Never null.
        /// </summary>
        public string Description { get; }

        /// <summary>
        /// Gets a warning about how the operation deviates from the mapping (for example a NOT NULL column added as
        /// nullable); null when none.
        /// </summary>
        public string? Warning { get; }

        /// <summary>
        /// Gets the statements to execute, in order. Never null or empty.
        /// </summary>
        public IReadOnlyList<SqlStatement> Statements { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an operation.
        /// </summary>
        /// <param name="kind">Kind of change.</param>
        /// <param name="tableName">Table name. Must not be null or empty.</param>
        /// <param name="columnName">Column name; null for non-column operations.</param>
        /// <param name="indexName">Index name; null for non-index operations.</param>
        /// <param name="isDestructive">Whether the operation is destructive.</param>
        /// <param name="description">Description. Must not be null.</param>
        /// <param name="statements">Statements. Must not be null or empty.</param>
        /// <param name="warning">Optional warning.</param>
        /// <exception cref="ArgumentException">Thrown when tableName is empty or statements is empty.</exception>
        /// <exception cref="ArgumentNullException">Thrown when description or statements is null.</exception>
        public MigrationOperation(
            MigrationOperationKind kind,
            string tableName,
            string? columnName,
            string? indexName,
            bool isDestructive,
            string description,
            IEnumerable<SqlStatement> statements,
            string? warning = null)
        {
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty.", nameof(tableName));
            ArgumentNullException.ThrowIfNull(statements);
            List<SqlStatement> list = statements.ToList();
            if (list.Count == 0) throw new ArgumentException("An operation needs at least one statement.", nameof(statements));
            Kind = kind;
            TableName = tableName;
            ColumnName = columnName;
            IndexName = indexName;
            IsDestructive = isDestructive;
            Description = description ?? throw new ArgumentNullException(nameof(description));
            Statements = list;
            Warning = warning;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the description, prefixed with "[destructive]" when applicable.
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return (IsDestructive ? "[destructive] " : string.Empty) + Description;
        }

        #endregion
    }
}
