namespace Durable.Sql
{
    using System;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A versioned schema change applied once per database by <see cref="SqlMigrator"/> and recorded in the history table.
    /// <para>
    /// <see cref="Id"/> must be stable forever and sort chronologically with ordinal string comparison, for example
    /// "20261005_0001_AddEmail". Implement <see cref="Up(MigrationContext)"/>; override <see cref="UpAsync"/> for a truly
    /// asynchronous implementation (used by <see cref="SqlMigrator.MigrateAsync"/>). Override <see cref="Down(MigrationContext)"/>
    /// (and optionally <see cref="DownAsync"/>) to support <see cref="SqlMigrator.RollbackTo(string?)"/>.
    /// </para>
    /// Thread safety: implementations should be stateless; one instance may be used by several migrators.
    /// </summary>
    public abstract class Migration
    {
        #region Public-Members

        /// <summary>
        /// Gets the unique, stable identifier. Ordinal order defines application order. Must not be null or empty and at
        /// most 150 characters.
        /// </summary>
        public abstract string Id { get; }

        /// <summary>
        /// Gets a human-readable description stored in the history table (truncated to 1000 characters).
        /// Default: the class name. May be null.
        /// </summary>
        public virtual string? Description => GetType().Name;

        /// <summary>
        /// Gets whether the migration runs inside a transaction (together with its history record) on databases with
        /// transactional DDL. Return false for statements that cannot run in a transaction (for example PostgreSQL's
        /// CREATE INDEX CONCURRENTLY). Note that on SQLite the transaction also serializes concurrent migrators.
        /// Default: true.
        /// </summary>
        public virtual bool UseTransaction => true;

        /// <summary>
        /// Gets whether <see cref="Down(MigrationContext)"/> or <see cref="DownAsync"/> is overridden.
        /// </summary>
        public bool SupportsDown
        {
            get
            {
                Type type = GetType();
                MethodInfo? down = type.GetMethod(nameof(Down), new[] { typeof(MigrationContext) });
                MethodInfo? downAsync = type.GetMethod(nameof(DownAsync), new[] { typeof(MigrationContext), typeof(CancellationToken) });
                return (down != null && down.DeclaringType != typeof(Migration)) || (downAsync != null && downAsync.DeclaringType != typeof(Migration));
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Applies the migration.
        /// </summary>
        /// <param name="context">Context for executing SQL and synchronizing schema. Never null.</param>
        public abstract void Up(MigrationContext context);

        /// <summary>
        /// Applies the migration asynchronously. Default: calls <see cref="Up(MigrationContext)"/>.
        /// </summary>
        /// <param name="context">Context for executing SQL and synchronizing schema. Never null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        public virtual Task UpAsync(MigrationContext context, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            Up(context);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Reverts the migration. Default: throws <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="context">Context for executing SQL and synchronizing schema. Never null.</param>
        /// <exception cref="NotSupportedException">Thrown when the migration does not support reverting.</exception>
        public virtual void Down(MigrationContext context)
        {
            throw new NotSupportedException("Migration " + Id + " does not support Down.");
        }

        /// <summary>
        /// Reverts the migration asynchronously. Default: calls <see cref="Down(MigrationContext)"/>.
        /// </summary>
        /// <param name="context">Context for executing SQL and synchronizing schema. Never null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="NotSupportedException">Thrown when the migration does not support reverting.</exception>
        public virtual Task DownAsync(MigrationContext context, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            Down(context);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Returns "Id: Description".
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return Id + (Description != null ? ": " + Description : string.Empty);
        }

        #endregion
    }
}
