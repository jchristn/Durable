namespace Test.Shared
{
    using System;
    using Durable.Sql;

    /// <summary>
    /// A migration whose Up (and optional Down) are supplied as delegates, so tests can declare migrations inline.
    /// Thread safety: immutable; the delegates determine thread safety.
    /// </summary>
    public class DelegateMigration : Migration
    {
        #region Public-Members

        /// <inheritdoc />
        public override string Id { get; }

        /// <inheritdoc />
        public override string? Description { get; }

        /// <inheritdoc />
        public override bool UseTransaction { get; }

        #endregion

        #region Private-Members

        private readonly Action<MigrationContext> _Up;
        private readonly Action<MigrationContext>? _Down;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the migration.
        /// </summary>
        /// <param name="id">Identifier. Must not be null or empty.</param>
        /// <param name="description">Description; may be null.</param>
        /// <param name="up">Up action. Must not be null.</param>
        /// <param name="down">Down action; null when the migration cannot be reverted.</param>
        /// <param name="useTransaction">Whether to run in a transaction. Default: true.</param>
        /// <exception cref="ArgumentNullException">Thrown when id or up is null.</exception>
        public DelegateMigration(string id, string? description, Action<MigrationContext> up, Action<MigrationContext>? down = null, bool useTransaction = true)
        {
            Id = id ?? throw new ArgumentNullException(nameof(id));
            Description = description;
            _Up = up ?? throw new ArgumentNullException(nameof(up));
            _Down = down;
            UseTransaction = useTransaction;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override void Up(MigrationContext context)
        {
            _Up(context);
        }

        /// <summary>
        /// Runs the Down delegate.
        /// </summary>
        /// <param name="context">Context. Never null.</param>
        /// <exception cref="NotSupportedException">Thrown when no Down delegate was supplied.</exception>
        public override void Down(MigrationContext context)
        {
            if (_Down == null) throw new NotSupportedException("Migration " + Id + " has no Down.");
            _Down(context);
        }

        #endregion
    }
}
