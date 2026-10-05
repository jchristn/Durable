namespace Durable.Sql
{
    using System;

    /// <summary>
    /// Thrown when a migration or schema synchronization fails. The failed migration is never recorded in the history
    /// table. When <see cref="MayBePartiallyApplied"/> is false the failed work was rolled back; when true (no transactional
    /// DDL, such as MySQL, or a migration with <see cref="Migration.UseTransaction"/> false) statements executed before the
    /// failure remain applied and must be reconciled manually before retrying.
    /// </summary>
    public class MigrationException : Exception
    {
        #region Public-Members

        /// <summary>
        /// Gets the identifier of the failed migration; null for a schema synchronization.
        /// </summary>
        public string? MigrationId { get; }

        /// <summary>
        /// Gets whether some statements of the failed work may remain applied.
        /// </summary>
        public bool MayBePartiallyApplied { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the exception.
        /// </summary>
        public MigrationException() : base("A migration failed.")
        {
        }

        /// <summary>
        /// Instantiates the exception.
        /// </summary>
        /// <param name="message">Message.</param>
        public MigrationException(string message) : base(message)
        {
        }

        /// <summary>
        /// Instantiates the exception.
        /// </summary>
        /// <param name="message">Message.</param>
        /// <param name="innerException">Cause; may be null.</param>
        public MigrationException(string message, Exception? innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Instantiates the exception.
        /// </summary>
        /// <param name="migrationId">Failed migration identifier; null for a schema synchronization.</param>
        /// <param name="message">Message.</param>
        /// <param name="mayBePartiallyApplied">Whether some statements may remain applied.</param>
        /// <param name="innerException">Cause; may be null.</param>
        public MigrationException(string? migrationId, string message, bool mayBePartiallyApplied, Exception? innerException) : base(message, innerException)
        {
            MigrationId = migrationId;
            MayBePartiallyApplied = mayBePartiallyApplied;
        }

        #endregion
    }
}
