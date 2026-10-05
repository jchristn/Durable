namespace Durable.Sql
{
    using System;

    /// <summary>
    /// Options for <see cref="SqlMigrator"/>.
    /// Thread safety: not thread-safe for mutation; configure before use and do not change while a migration runs.
    /// </summary>
    public class SqlMigratorOptions
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the history table name (letters, digits and underscores; resolved in the connection's default schema).
        /// Default: "__durable_migrations".
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when the value is not a valid identifier (at most 128 characters).</exception>
        public string HistoryTableName
        {
            get => _HistoryTableName;
            set => _HistoryTableName = SqlIdentifierValidator.RequireIdentifier(value, nameof(value));
        }

        /// <summary>
        /// Gets or sets the name of the database lock that serializes migrators (PostgreSQL advisory lock, SQL Server
        /// application lock, MySQL named lock). Null derives "durable_migrations_" + <see cref="HistoryTableName"/>,
        /// truncated to 64 characters. Default: null.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when the value is empty or longer than 64 characters.</exception>
        public string? LockName
        {
            get => _LockName;
            set
            {
                if (value != null && (value.Length == 0 || value.Length > 64))
                    throw new ArgumentException("LockName must be between 1 and 64 characters.", nameof(value));
                _LockName = value;
            }
        }

        /// <summary>
        /// Gets or sets how long to wait for the migration lock before throwing <see cref="TimeoutException"/>.
        /// Default: 120. Minimum: 1. Maximum: 86400.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is outside the allowed range.</exception>
        public int LockTimeoutSeconds
        {
            get => _LockTimeoutSeconds;
            set
            {
                if (value < 1 || value > 86400) throw new ArgumentOutOfRangeException(nameof(value), "LockTimeoutSeconds must be between 1 and 86400.");
                _LockTimeoutSeconds = value;
            }
        }

        /// <summary>
        /// Gets or sets the delay between lock attempts on databases whose lock function does not wait (PostgreSQL).
        /// Default: 250. Minimum: 10. Maximum: 60000.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is outside the allowed range.</exception>
        public int LockPollIntervalMilliseconds
        {
            get => _LockPollIntervalMilliseconds;
            set
            {
                if (value < 10 || value > 60000) throw new ArgumentOutOfRangeException(nameof(value), "LockPollIntervalMilliseconds must be between 10 and 60000.");
                _LockPollIntervalMilliseconds = value;
            }
        }

        /// <summary>
        /// Gets or sets whether each migration runs in a transaction together with its history record on databases with
        /// transactional DDL (<see cref="ISqlDialect.SupportsTransactionalDdl"/>). Individual migrations can opt out with
        /// <see cref="Migration.UseTransaction"/>. Default: true.
        /// </summary>
        public bool UseTransactions { get; set; } = true;

        /// <summary>
        /// Gets or sets command options (timeout, interceptors, logger, tracing) applied to every statement the migrator
        /// executes. Null uses defaults. Default: null.
        /// </summary>
        public SqlRepositoryOptions? CommandOptions { get; set; } = null;

        /// <summary>
        /// Gets or sets a callback invoked after each statement the migrator executes (on the migrating thread), for
        /// example to log or capture SQL. Null for none. Default: null.
        /// </summary>
        public Action<SqlStatement>? StatementExecuted { get; set; } = null;

        #endregion

        #region Private-Members

        private string _HistoryTableName = "__durable_migrations";
        private string? _LockName = null;
        private int _LockTimeoutSeconds = 120;
        private int _LockPollIntervalMilliseconds = 250;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the effective lock name: <see cref="LockName"/>, or the name derived from <see cref="HistoryTableName"/>.
        /// </summary>
        /// <returns>The lock name; at most 64 characters.</returns>
        public string GetEffectiveLockName()
        {
            if (_LockName != null) return _LockName;
            string derived = "durable_migrations_" + _HistoryTableName;
            return derived.Length > 64 ? derived.Substring(0, 64) : derived;
        }

        #endregion
    }
}
