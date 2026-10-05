namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Lightweight migrations without model snapshots: applies versioned <see cref="Migration"/>s once per database
    /// (recorded in a history table) and synchronizes entity schemas additively.
    /// <para>
    /// Concurrency: every run that changes the database first takes a database-level lock through the dialect
    /// (PostgreSQL advisory lock, SQL Server application lock, MySQL named lock), so several processes can call
    /// <see cref="Migrate"/> at once and each migration is applied exactly once. SQLite has no session lock; there each
    /// migration's write transaction (BEGIN IMMEDIATE) serializes migrators, and the history is re-checked inside it.
    /// </para>
    /// <para>
    /// Transactions: on databases with transactional DDL each migration runs in a transaction together with its history
    /// record, so a failed migration is rolled back and not recorded. On MySQL DDL commits implicitly: a failed migration
    /// is not recorded but statements before the failure remain (reported through
    /// <see cref="MigrationException.MayBePartiallyApplied"/>); keep MySQL migrations small and idempotent.
    /// </para>
    /// Thread safety: registering migrations is not thread-safe; configure before use. Running operations is safe to call
    /// concurrently (they serialize through the database lock), but each call uses its own connection.
    /// </summary>
    public class SqlMigrator
    {
        #region Public-Members

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets the connection factory. The migrator never disposes it. Never null.
        /// </summary>
        public IConnectionFactory ConnectionFactory { get; }

        /// <summary>
        /// Gets the options. Never null.
        /// </summary>
        public SqlMigratorOptions Options { get; }

        /// <summary>
        /// Gets the registered migrations in application (ordinal <see cref="Migration.Id"/>) order. Never null.
        /// </summary>
        public IReadOnlyList<Migration> Migrations => _Migrations.OrderBy(m => m.Id, StringComparer.Ordinal).ToList();

        #endregion

        #region Private-Members

        private readonly List<Migration> _Migrations = new List<Migration>();
        private readonly int _MaxIdLength = 150;
        private readonly int _MaxDescriptionLength = 1000;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a migrator.
        /// </summary>
        /// <param name="connectionFactory">Connection factory. Must not be null.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionFactory or dialect is null.</exception>
        public SqlMigrator(IConnectionFactory connectionFactory, ISqlDialect dialect, SqlMigratorOptions? options = null)
        {
            ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            Options = options ?? new SqlMigratorOptions();
        }

        /// <summary>
        /// Instantiates a migrator with an explicit list of migrations.
        /// </summary>
        /// <param name="connectionFactory">Connection factory. Must not be null.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="migrations">Migrations. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument other than options is null.</exception>
        /// <exception cref="ArgumentException">Thrown when a migration identifier is invalid or duplicated.</exception>
        public SqlMigrator(IConnectionFactory connectionFactory, ISqlDialect dialect, IEnumerable<Migration> migrations, SqlMigratorOptions? options = null)
            : this(connectionFactory, dialect, options)
        {
            AddMigrations(migrations);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Registers a migration.
        /// </summary>
        /// <param name="migration">Migration. Must not be null.</param>
        /// <returns>This migrator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when migration is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the identifier is empty, longer than 150 characters, or already registered.</exception>
        public SqlMigrator AddMigration(Migration migration)
        {
            ArgumentNullException.ThrowIfNull(migration);
            string id = migration.Id;
            if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("Migration " + migration.GetType().Name + " has an empty Id.", nameof(migration));
            if (id.Length > _MaxIdLength) throw new ArgumentException("Migration Id '" + id + "' is longer than " + _MaxIdLength + " characters.", nameof(migration));
            if (_Migrations.Any(m => string.Equals(m.Id, id, StringComparison.Ordinal)))
                throw new ArgumentException("A migration with Id '" + id + "' is already registered.", nameof(migration));
            _Migrations.Add(migration);
            return this;
        }

        /// <summary>
        /// Registers migrations.
        /// </summary>
        /// <param name="migrations">Migrations. Must not be null.</param>
        /// <returns>This migrator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when migrations is null.</exception>
        /// <exception cref="ArgumentException">Thrown when an identifier is invalid or duplicated.</exception>
        public SqlMigrator AddMigrations(IEnumerable<Migration> migrations)
        {
            ArgumentNullException.ThrowIfNull(migrations);
            foreach (Migration migration in migrations) AddMigration(migration);
            return this;
        }

        /// <summary>
        /// Registers every concrete <see cref="Migration"/> subclass in an assembly that has a public parameterless constructor.
        /// </summary>
        /// <param name="assembly">Assembly. Must not be null.</param>
        /// <param name="filter">Optional type filter (for example by namespace); null registers all.</param>
        /// <returns>This migrator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when assembly is null.</exception>
        /// <exception cref="ArgumentException">Thrown when an identifier is invalid or duplicated.</exception>
        public SqlMigrator AddMigrationsFromAssembly(Assembly assembly, Func<Type, bool>? filter = null)
        {
            ArgumentNullException.ThrowIfNull(assembly);
            IEnumerable<Type> types = assembly.GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && !t.ContainsGenericParameters && typeof(Migration).IsAssignableFrom(t) && t.GetConstructor(Type.EmptyTypes) != null)
                .Where(t => filter == null || filter(t))
                .OrderBy(t => t.FullName, StringComparer.Ordinal);
            foreach (Type type in types) AddMigration((Migration)Activator.CreateInstance(type)!);
            return this;
        }

        /// <summary>
        /// Reads the history table.
        /// </summary>
        /// <returns>Applied migrations in identifier order; empty when the history table does not exist.</returns>
        public List<AppliedMigration> GetAppliedMigrations()
        {
            using MigrationSession session = MigrationSession.Open(CreateExecutor());
            return ReadHistory(session);
        }

        /// <summary>
        /// Reads the history table.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Applied migrations in identifier order; empty when the history table does not exist.</returns>
        public async Task<List<AppliedMigration>> GetAppliedMigrationsAsync(CancellationToken token = default)
        {
            MigrationSession session = await MigrationSession.OpenAsync(CreateExecutor(), token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                return await ReadHistoryAsync(session, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns the registered migrations that are not recorded in the history table, in application order.
        /// </summary>
        /// <returns>Pending migrations.</returns>
        public List<Migration> GetPendingMigrations()
        {
            HashSet<string> applied = new HashSet<string>(GetAppliedMigrations().Select(a => a.Id), StringComparer.Ordinal);
            return Migrations.Where(m => !applied.Contains(m.Id)).ToList();
        }

        /// <summary>
        /// Returns the registered migrations that are not recorded in the history table, in application order.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Pending migrations.</returns>
        public async Task<List<Migration>> GetPendingMigrationsAsync(CancellationToken token = default)
        {
            List<AppliedMigration> history = await GetAppliedMigrationsAsync(token).ConfigureAwait(false);
            HashSet<string> applied = new HashSet<string>(history.Select(a => a.Id), StringComparer.Ordinal);
            return Migrations.Where(m => !applied.Contains(m.Id)).ToList();
        }

        /// <summary>
        /// Applies all pending migrations in identifier order under the migration lock, recording each in the history table.
        /// Stops at the first failure.
        /// </summary>
        /// <returns>The migrations applied (and any skipped because a concurrent migrator applied them first).</returns>
        /// <exception cref="MigrationException">Thrown when a migration fails; it is not recorded.</exception>
        /// <exception cref="TimeoutException">Thrown when the migration lock cannot be acquired in time.</exception>
        public MigrationRunResult Migrate()
        {
            List<Migration> migrations = Migrations.ToList();
            List<AppliedMigration> applied = new List<AppliedMigration>();
            List<string> skipped = new List<string>();
            using MigrationSession session = MigrationSession.Open(CreateExecutor());
            session.AcquireLock(Options.GetEffectiveLockName(), Options.LockTimeoutSeconds, Options.LockPollIntervalMilliseconds, CancellationToken.None);
            try
            {
                session.Execute(new SqlStatement(Dialect.CreateMigrationHistoryTableSql(Options.HistoryTableName)), "MIGRATION HISTORY");
                HashSet<string> done = new HashSet<string>(ReadHistory(session).Select(a => a.Id), StringComparer.Ordinal);
                foreach (Migration migration in migrations)
                {
                    if (done.Contains(migration.Id)) continue;
                    AppliedMigration? result = RunOne(session, migration, false);
                    if (result != null) applied.Add(result);
                    else skipped.Add(migration.Id);
                }
            }
            finally
            {
                session.Rollback();
                session.ReleaseLock();
            }

            return new MigrationRunResult(applied, new List<string>(), skipped);
        }

        /// <summary>
        /// Applies all pending migrations in identifier order under the migration lock, recording each in the history table.
        /// Stops at the first failure.
        /// </summary>
        /// <param name="token">Cancellation token. Cancellation rolls back the running migration where possible.</param>
        /// <returns>The migrations applied (and any skipped because a concurrent migrator applied them first).</returns>
        /// <exception cref="MigrationException">Thrown when a migration fails; it is not recorded.</exception>
        /// <exception cref="TimeoutException">Thrown when the migration lock cannot be acquired in time.</exception>
        public async Task<MigrationRunResult> MigrateAsync(CancellationToken token = default)
        {
            List<Migration> migrations = Migrations.ToList();
            List<AppliedMigration> applied = new List<AppliedMigration>();
            List<string> skipped = new List<string>();
            MigrationSession session = await MigrationSession.OpenAsync(CreateExecutor(), token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                await session.AcquireLockAsync(Options.GetEffectiveLockName(), Options.LockTimeoutSeconds, Options.LockPollIntervalMilliseconds, token).ConfigureAwait(false);
                try
                {
                    await session.ExecuteAsync(new SqlStatement(Dialect.CreateMigrationHistoryTableSql(Options.HistoryTableName)), "MIGRATION HISTORY", token).ConfigureAwait(false);
                    List<AppliedMigration> history = await ReadHistoryAsync(session, token).ConfigureAwait(false);
                    HashSet<string> done = new HashSet<string>(history.Select(a => a.Id), StringComparer.Ordinal);
                    foreach (Migration migration in migrations)
                    {
                        if (done.Contains(migration.Id)) continue;
                        AppliedMigration? result = await RunOneAsync(session, migration, false, token).ConfigureAwait(false);
                        if (result != null) applied.Add(result);
                        else skipped.Add(migration.Id);
                    }
                }
                finally
                {
                    await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    await session.ReleaseLockAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }

            return new MigrationRunResult(applied, new List<string>(), skipped);
        }

        /// <summary>
        /// Reverts applied migrations whose identifier sorts after targetId, newest first, using
        /// <see cref="Migration.Down(MigrationContext)"/>, and removes their history records.
        /// </summary>
        /// <param name="targetId">Last migration to keep; null reverts every applied migration.</param>
        /// <returns>The reverted migration identifiers.</returns>
        /// <exception cref="InvalidOperationException">Thrown before any change when an applied migration to revert is not
        /// registered or does not support Down.</exception>
        /// <exception cref="MigrationException">Thrown when a Down fails; its history record is kept.</exception>
        /// <exception cref="TimeoutException">Thrown when the migration lock cannot be acquired in time.</exception>
        public MigrationRunResult RollbackTo(string? targetId)
        {
            List<string> reverted = new List<string>();
            List<string> skipped = new List<string>();
            using MigrationSession session = MigrationSession.Open(CreateExecutor());
            session.AcquireLock(Options.GetEffectiveLockName(), Options.LockTimeoutSeconds, Options.LockPollIntervalMilliseconds, CancellationToken.None);
            try
            {
                foreach (Migration migration in PlanRollback(ReadHistory(session), targetId))
                {
                    if (RunOne(session, migration, true) != null) reverted.Add(migration.Id);
                    else skipped.Add(migration.Id);
                }
            }
            finally
            {
                session.Rollback();
                session.ReleaseLock();
            }

            return new MigrationRunResult(new List<AppliedMigration>(), reverted, skipped);
        }

        /// <summary>
        /// Reverts applied migrations whose identifier sorts after targetId, newest first, using
        /// <see cref="Migration.DownAsync"/>, and removes their history records.
        /// </summary>
        /// <param name="targetId">Last migration to keep; null reverts every applied migration.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The reverted migration identifiers.</returns>
        /// <exception cref="InvalidOperationException">Thrown before any change when an applied migration to revert is not
        /// registered or does not support Down.</exception>
        /// <exception cref="MigrationException">Thrown when a Down fails; its history record is kept.</exception>
        /// <exception cref="TimeoutException">Thrown when the migration lock cannot be acquired in time.</exception>
        public async Task<MigrationRunResult> RollbackToAsync(string? targetId, CancellationToken token = default)
        {
            List<string> reverted = new List<string>();
            List<string> skipped = new List<string>();
            MigrationSession session = await MigrationSession.OpenAsync(CreateExecutor(), token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                await session.AcquireLockAsync(Options.GetEffectiveLockName(), Options.LockTimeoutSeconds, Options.LockPollIntervalMilliseconds, token).ConfigureAwait(false);
                try
                {
                    List<AppliedMigration> history = await ReadHistoryAsync(session, token).ConfigureAwait(false);
                    foreach (Migration migration in PlanRollback(history, targetId))
                    {
                        if (await RunOneAsync(session, migration, true, token).ConfigureAwait(false) != null) reverted.Add(migration.Id);
                        else skipped.Add(migration.Id);
                    }
                }
                finally
                {
                    await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    await session.ReleaseLockAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }

            return new MigrationRunResult(new List<AppliedMigration>(), reverted, skipped);
        }

        /// <summary>
        /// Generates a SQL script that creates the history table (if missing) and applies the currently pending migrations,
        /// each followed by its history insert and wrapped in a transaction where the database supports transactional DDL.
        /// Nothing is executed. Statements issued with <see cref="MigrationContext.ExecuteSql"/> are scripted with their
        /// parameters inlined; <see cref="MigrationContext.EnsureSchema(Type[])"/> is evaluated against the database as it is
        /// now, so a later migration that depends on an earlier pending one may script differently than it would run.
        /// </summary>
        /// <returns>The script.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a migration reads data while scripting.</exception>
        public string GenerateScript()
        {
            using MigrationSession session = MigrationSession.Open(CreateExecutor());
            HashSet<string> done = new HashSet<string>(ReadHistory(session).Select(a => a.Id), StringComparer.Ordinal);
            StringBuilder script = StartScript();
            foreach (Migration migration in Migrations.Where(m => !done.Contains(m.Id)))
            {
                bool transactional = BeginScriptedMigration(script, migration);
                migration.Up(new MigrationContext(session, migration, script));
                EndScriptedMigration(script, migration, transactional);
            }

            return script.ToString();
        }

        /// <summary>
        /// Generates a SQL script that creates the history table (if missing) and applies the currently pending migrations.
        /// See <see cref="GenerateScript"/>.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The script.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a migration reads data while scripting.</exception>
        public async Task<string> GenerateScriptAsync(CancellationToken token = default)
        {
            MigrationSession session = await MigrationSession.OpenAsync(CreateExecutor(), token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                List<AppliedMigration> history = await ReadHistoryAsync(session, token).ConfigureAwait(false);
                HashSet<string> done = new HashSet<string>(history.Select(a => a.Id), StringComparer.Ordinal);
                StringBuilder script = StartScript();
                foreach (Migration migration in Migrations.Where(m => !done.Contains(m.Id)))
                {
                    bool transactional = BeginScriptedMigration(script, migration);
                    await migration.UpAsync(new MigrationContext(session, migration, script), token).ConfigureAwait(false);
                    EndScriptedMigration(script, migration, transactional);
                }

                return script.ToString();
            }
        }

        /// <summary>
        /// Compares entity types with the live schema without changing anything.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>The diff.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public SchemaDiff DiffSchema(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            List<Type> types = entityTypes.ToList();
            using MigrationSession session = MigrationSession.Open(CreateExecutor());
            return SchemaSynchronizer.Diff(session, types, options ?? new SchemaSyncOptions());
        }

        /// <summary>
        /// Compares entity types with the live schema without changing anything.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The diff.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public async Task<SchemaDiff> DiffSchemaAsync(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            List<Type> types = entityTypes.ToList();
            MigrationSession session = await MigrationSession.OpenAsync(CreateExecutor(), token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                return await SchemaSynchronizer.DiffAsync(session, types, options ?? new SchemaSyncOptions(), token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Renders the schema diff for entity types as a SQL script (destructive operations included only when
        /// <see cref="SchemaSyncOptions.AllowDestructive"/> is true). Nothing is executed.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <returns>The script.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public string GenerateSyncScript(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null)
        {
            options ??= new SchemaSyncOptions();
            return DiffSchema(entityTypes, options).ToScript(options.AllowDestructive);
        }

        /// <summary>
        /// Renders the schema diff for entity types as a SQL script. Nothing is executed.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The script.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public async Task<string> GenerateSyncScriptAsync(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null, CancellationToken token = default)
        {
            options ??= new SchemaSyncOptions();
            SchemaDiff diff = await DiffSchemaAsync(entityTypes, options, token).ConfigureAwait(false);
            return diff.ToScript(options.AllowDestructive);
        }

        /// <summary>
        /// Synchronizes the tables of entity types with their mappings under the migration lock: creates missing tables,
        /// adds missing columns and creates missing indexes; destructive operations only when
        /// <see cref="SchemaSyncOptions.AllowDestructive"/> is true. Differences that need a manual step are reported in the
        /// result, never applied. Runs in one transaction on databases with transactional DDL. Idempotent.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults (additive only).</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        /// <exception cref="MigrationException">Thrown when applying an operation fails.</exception>
        /// <exception cref="TimeoutException">Thrown when the migration lock cannot be acquired in time.</exception>
        public SchemaSyncResult SyncSchema(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            options ??= new SchemaSyncOptions();
            List<Type> types = entityTypes.ToList();
            bool transactional = options.UseTransaction && Dialect.SupportsTransactionalDdl;
            List<MigrationOperation> applied = new List<MigrationOperation>();
            using MigrationSession session = MigrationSession.Open(CreateExecutor());
            session.AcquireLock(Options.GetEffectiveLockName(), Options.LockTimeoutSeconds, Options.LockPollIntervalMilliseconds, CancellationToken.None);
            try
            {
                if (transactional) session.Begin();
                SchemaDiff diff = SchemaSynchronizer.Diff(session, types, options);
                try
                {
                    SchemaSyncResult result = SchemaSynchronizer.Apply(session, diff, options, applied);
                    session.Commit();
                    return result;
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    session.Rollback();
                    throw SyncFailure(applied, transactional, e);
                }
            }
            finally
            {
                session.Rollback();
                session.ReleaseLock();
            }
        }

        /// <summary>
        /// Synchronizes the tables of entity types with their mappings under the migration lock. See
        /// <see cref="SyncSchema(IEnumerable{Type}, SchemaSyncOptions?)"/>.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="options">Options; null uses defaults (additive only).</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        /// <exception cref="MigrationException">Thrown when applying an operation fails.</exception>
        /// <exception cref="TimeoutException">Thrown when the migration lock cannot be acquired in time.</exception>
        public async Task<SchemaSyncResult> SyncSchemaAsync(IEnumerable<Type> entityTypes, SchemaSyncOptions? options = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            options ??= new SchemaSyncOptions();
            List<Type> types = entityTypes.ToList();
            bool transactional = options.UseTransaction && Dialect.SupportsTransactionalDdl;
            List<MigrationOperation> applied = new List<MigrationOperation>();
            MigrationSession session = await MigrationSession.OpenAsync(CreateExecutor(), token).ConfigureAwait(false);
            await using (session.ConfigureAwait(false))
            {
                await session.AcquireLockAsync(Options.GetEffectiveLockName(), Options.LockTimeoutSeconds, Options.LockPollIntervalMilliseconds, token).ConfigureAwait(false);
                try
                {
                    if (transactional) await session.BeginAsync(token).ConfigureAwait(false);
                    SchemaDiff diff = await SchemaSynchronizer.DiffAsync(session, types, options, token).ConfigureAwait(false);
                    try
                    {
                        SchemaSyncResult result = await SchemaSynchronizer.ApplyAsync(session, diff, options, applied, token).ConfigureAwait(false);
                        await session.CommitAsync(token).ConfigureAwait(false);
                        return result;
                    }
                    catch (Exception e) when (e is not OperationCanceledException)
                    {
                        await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                        throw SyncFailure(applied, transactional, e);
                    }
                }
                finally
                {
                    await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                    await session.ReleaseLockAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
        }

        #endregion

        #region Private-Methods

        private SqlCommandExecutor CreateExecutor()
        {
            return DatabaseSchemaReader.CreateExecutor(Dialect, ConnectionFactory, Options.CommandOptions, Options.HistoryTableName, Options.StatementExecuted);
        }

        private bool UsesTransaction(Migration migration)
        {
            return Options.UseTransactions && Dialect.SupportsTransactionalDdl && migration.UseTransaction;
        }

        private AppliedMigration? RunOne(MigrationSession session, Migration migration, bool down)
        {
            bool transactional = UsesTransaction(migration);
            if (transactional) session.Begin();
            try
            {
                bool recorded = IsRecorded(session, migration.Id);
                if (recorded == down)
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    MigrationContext context = new MigrationContext(session, migration, null);
                    if (down) migration.Down(context);
                    else migration.Up(context);
                    stopwatch.Stop();
                    DateTime now = DateTime.UtcNow;
                    session.Execute(down ? DeleteHistoryStatement(migration.Id) : InsertHistoryStatement(migration, now, stopwatch.ElapsedMilliseconds), "MIGRATION HISTORY");
                    session.Commit();
                    return new AppliedMigration(migration.Id, Truncate(migration.Description), now, stopwatch.ElapsedMilliseconds);
                }

                session.Rollback();
                return null;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                session.Rollback();
                throw MigrationFailure(migration, down, transactional, e);
            }
            catch
            {
                session.Rollback();
                throw;
            }
        }

        private async Task<AppliedMigration?> RunOneAsync(MigrationSession session, Migration migration, bool down, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            bool transactional = UsesTransaction(migration);
            if (transactional) await session.BeginAsync(token).ConfigureAwait(false);
            try
            {
                bool recorded = await IsRecordedAsync(session, migration.Id, token).ConfigureAwait(false);
                if (recorded == down)
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    MigrationContext context = new MigrationContext(session, migration, null);
                    if (down) await migration.DownAsync(context, token).ConfigureAwait(false);
                    else await migration.UpAsync(context, token).ConfigureAwait(false);
                    stopwatch.Stop();
                    DateTime now = DateTime.UtcNow;
                    SqlStatement history = down ? DeleteHistoryStatement(migration.Id) : InsertHistoryStatement(migration, now, stopwatch.ElapsedMilliseconds);
                    await session.ExecuteAsync(history, "MIGRATION HISTORY", token).ConfigureAwait(false);
                    await session.CommitAsync(token).ConfigureAwait(false);
                    return new AppliedMigration(migration.Id, Truncate(migration.Description), now, stopwatch.ElapsedMilliseconds);
                }

                await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                return null;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                throw MigrationFailure(migration, down, transactional, e);
            }
            catch
            {
                await session.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                throw;
            }
        }

        private List<Migration> PlanRollback(List<AppliedMigration> history, string? targetId)
        {
            List<Migration> plan = new List<Migration>();
            foreach (AppliedMigration applied in history.OrderByDescending(a => a.Id, StringComparer.Ordinal))
            {
                if (targetId != null && string.CompareOrdinal(applied.Id, targetId) <= 0) continue;
                Migration? migration = _Migrations.FirstOrDefault(m => string.Equals(m.Id, applied.Id, StringComparison.Ordinal));
                if (migration == null)
                    throw new InvalidOperationException("Applied migration '" + applied.Id + "' is not registered; it cannot be reverted.");
                if (!migration.SupportsDown)
                    throw new InvalidOperationException("Migration '" + applied.Id + "' does not override Down; it cannot be reverted.");
                plan.Add(migration);
            }

            return plan;
        }

        private List<AppliedMigration> ReadHistory(MigrationSession session)
        {
            if (!DatabaseSchemaReader.TableExists(session, Options.HistoryTableName)) return new List<AppliedMigration>();
            return session.Query(SelectHistoryStatement(), "MIGRATION HISTORY", ReadHistoryRow).OrderBy(a => a.Id, StringComparer.Ordinal).ToList();
        }

        private async Task<List<AppliedMigration>> ReadHistoryAsync(MigrationSession session, CancellationToken token)
        {
            if (!await DatabaseSchemaReader.TableExistsAsync(session, Options.HistoryTableName, token).ConfigureAwait(false)) return new List<AppliedMigration>();
            List<AppliedMigration> rows = await session.QueryAsync(SelectHistoryStatement(), "MIGRATION HISTORY", ReadHistoryRow, token).ConfigureAwait(false);
            return rows.OrderBy(a => a.Id, StringComparer.Ordinal).ToList();
        }

        private bool IsRecorded(MigrationSession session, string id)
        {
            return session.Query(RecordedStatement(id), "MIGRATION HISTORY", r => 1).Count > 0;
        }

        private async Task<bool> IsRecordedAsync(MigrationSession session, string id, CancellationToken token)
        {
            List<int> rows = await session.QueryAsync(RecordedStatement(id), "MIGRATION HISTORY", r => 1, token).ConfigureAwait(false);
            return rows.Count > 0;
        }

        private AppliedMigration ReadHistoryRow(DbDataReader reader)
        {
            string id = Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture) ?? string.Empty;
            object description = reader.GetValue(1);
            object? applied = Dialect.Converter.ConvertFromDatabase(reader.GetValue(2), typeof(DateTime));
            object duration = reader.GetValue(3);
            return new AppliedMigration(
                id,
                description == DBNull.Value ? null : Convert.ToString(description, CultureInfo.InvariantCulture),
                applied is DateTime when ? when : DateTime.MinValue,
                duration == DBNull.Value ? 0 : Convert.ToInt64(duration, CultureInfo.InvariantCulture));
        }

        private string History => Dialect.QuoteIdentifier(Options.HistoryTableName);

        private string Column(string name) => Dialect.QuoteIdentifier(name);

        private SqlStatement SelectHistoryStatement()
        {
            return new SqlStatement(
                "SELECT " + Column("id") + ", " + Column("description") + ", " + Column("applied_utc") + ", " + Column("duration_ms") + " FROM " + History);
        }

        private SqlStatement RecordedStatement(string id)
        {
            return RawSql.Positional(
                "SELECT 1 FROM " + History + " WHERE " + Column("id") + " = " + Dialect.FormatParameterName(0),
                new object?[] { id }, Dialect, Dialect.Converter);
        }

        private SqlStatement InsertHistoryStatement(Migration migration, DateTime appliedUtc, long durationMs)
        {
            return RawSql.Positional(
                "INSERT INTO " + History + " (" + Column("id") + ", " + Column("description") + ", " + Column("applied_utc") + ", " + Column("duration_ms") + ") VALUES (" +
                Dialect.FormatParameterName(0) + ", " + Dialect.FormatParameterName(1) + ", " + Dialect.FormatParameterName(2) + ", " + Dialect.FormatParameterName(3) + ")",
                new object?[] { migration.Id, Truncate(migration.Description), appliedUtc, durationMs }, Dialect, Dialect.Converter);
        }

        private SqlStatement DeleteHistoryStatement(string id)
        {
            return RawSql.Positional(
                "DELETE FROM " + History + " WHERE " + Column("id") + " = " + Dialect.FormatParameterName(0),
                new object?[] { id }, Dialect, Dialect.Converter);
        }

        private StringBuilder StartScript()
        {
            StringBuilder script = new StringBuilder();
            MigrationScriptWriter.AppendComment(script, "Durable migration script (" + Dialect.RepositoryType.DisplayName + ")");
            MigrationScriptWriter.AppendStatement(script, Dialect, new SqlStatement(Dialect.CreateMigrationHistoryTableSql(Options.HistoryTableName)));
            return script;
        }

        private bool BeginScriptedMigration(StringBuilder script, Migration migration)
        {
            script.AppendLine();
            MigrationScriptWriter.AppendComment(script, "Migration " + migration);
            bool transactional = UsesTransaction(migration);
            if (transactional) MigrationScriptWriter.AppendStatement(script, Dialect, new SqlStatement(Dialect.ScriptBeginTransactionSql));
            return transactional;
        }

        private void EndScriptedMigration(StringBuilder script, Migration migration, bool transactional)
        {
            string insert = "INSERT INTO " + History + " (" + Column("id") + ", " + Column("description") + ", " + Column("applied_utc") + ", " + Column("duration_ms") + ") VALUES (" +
                Dialect.FormatLiteral(migration.Id) + ", " + Dialect.FormatLiteral(Truncate(migration.Description)) + ", " + Dialect.CurrentUtcTimestampSql + ", 0)";
            MigrationScriptWriter.AppendStatement(script, Dialect, new SqlStatement(insert));
            if (transactional) MigrationScriptWriter.AppendStatement(script, Dialect, new SqlStatement(Dialect.ScriptCommitTransactionSql));
        }

        private string? Truncate(string? description)
        {
            if (description == null) return null;
            return description.Length > _MaxDescriptionLength ? description.Substring(0, _MaxDescriptionLength) : description;
        }

        private MigrationException MigrationFailure(Migration migration, bool down, bool transactional, Exception cause)
        {
            string action = down ? "Reverting" : "Applying";
            string outcome = transactional
                ? "Its changes were rolled back and the history was not changed."
                : "It ran without a transaction (" + (Dialect.SupportsTransactionalDdl ? "UseTransaction is disabled" : Dialect.RepositoryType.DisplayName + " commits DDL implicitly") +
                  "), so statements executed before the failure may remain applied; the history was not changed. Reconcile the schema before retrying.";
            return new MigrationException(migration.Id, action + " migration " + migration + " failed: " + cause.Message + " " + outcome, !transactional, cause);
        }

        private MigrationException SyncFailure(List<MigrationOperation> applied, bool transactional, Exception cause)
        {
            string outcome = transactional
                ? "All changes were rolled back."
                : applied.Count + " operation(s) had been applied and remain: " + string.Join("; ", applied.Select(o => o.Description)) + ".";
            return new MigrationException(null, "Schema synchronization failed: " + cause.Message + " " + outcome, !transactional && applied.Count > 0, cause);
        }

        #endregion
    }
}
