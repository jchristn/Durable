namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// One run of a command: its parsed arguments, resolved settings and context, plus the services commands share
    /// (opening the database, loading the user's assembly, creating a migrator).
    /// </summary>
    internal sealed class CommandInvocation
    {
        /// <summary>Gets the command.</summary>
        public CommandDefinition Command { get; }

        /// <summary>Gets the parsed arguments.</summary>
        public ParsedArguments Arguments { get; }

        /// <summary>Gets the resolved settings.</summary>
        public ToolSettings Settings { get; }

        /// <summary>Gets the invocation context.</summary>
        public DurableCliContext Context { get; }

        /// <summary>Gets the output stream.</summary>
        public TextWriter Output => Context.Output;

        /// <summary>Gets the error stream.</summary>
        public TextWriter Error => Context.Error;

        /// <summary>Gets whether a provider and connection string are configured.</summary>
        public bool HasDatabase => Settings.Provider != null && Settings.Connection != null;

        private ProjectInfo? _Project;
        private UserAssembly? _Assembly;

        /// <summary>
        /// Instantiates an invocation.
        /// </summary>
        /// <param name="command">Command. Must not be null.</param>
        /// <param name="arguments">Parsed arguments. Must not be null.</param>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="context">Context. Must not be null.</param>
        public CommandInvocation(CommandDefinition command, ParsedArguments arguments, ToolSettings settings, DurableCliContext context)
        {
            Command = command ?? throw new ArgumentNullException(nameof(command));
            Arguments = arguments ?? throw new ArgumentNullException(nameof(arguments));
            Settings = settings ?? throw new ArgumentNullException(nameof(settings));
            Context = context ?? throw new ArgumentNullException(nameof(context));
        }

        /// <summary>
        /// Creates the database target from the settings. For DuckDB, whose native library the tool does not bundle, the
        /// user's build output is made known to <see cref="DuckDbNativeLibrary"/> first: the user's assembly is loaded (and
        /// built) when the command uses it, otherwise the output of the project, when one is found and built, is probed.
        /// </summary>
        /// <param name="loadsAssembly">Whether the command loads the user's assembly anyway.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The target; dispose it when done.</returns>
        /// <exception cref="DurableCliException">Thrown when the provider or connection string is missing or invalid, or the
        /// provider's native library cannot be found.</exception>
        public async Task<DatabaseTarget> OpenDatabaseAsync(bool loadsAssembly, CancellationToken token)
        {
            if (Settings.Provider == null)
                throw new DurableCliException("No database provider. Pass --provider <" + DatabaseTarget.ProviderChoices + ">, set DURABLE_PROVIDER, or add \"provider\" to durable.json.", null, true);
            if (Settings.Connection == null)
                throw new DurableCliException("No connection string. Pass --connection \"<connection string>\", set DURABLE_CONNECTION, or add \"connection\" to durable.json.", null, true);
            if (DatabaseTarget.NormalizeProvider(Settings.Provider) == "duckdb")
            {
                if (loadsAssembly) await LoadAssemblyAsync(token).ConfigureAwait(false);
                else await AddProjectOutputProbeAsync(token).ConfigureAwait(false);
                string? nativeOverride = Context.GetEnvironmentVariable(DuckDbNativeLibrary.EnvironmentVariable);
                DuckDbNativeLibrary.EnsureLoaded(string.IsNullOrWhiteSpace(nativeOverride) ? null : Context.ResolvePath(nativeOverride.Trim()));
            }

            DatabaseTarget target = DatabaseTarget.Create(Settings.Provider, Settings.Connection);
            if (Settings.Verbose) Error.WriteLine("Using " + target.DisplayName + ".");
            return target;
        }

        /// <summary>
        /// Returns the user's project, or null when <c>--assembly</c> is used or no project can be found and none is required.
        /// </summary>
        /// <param name="required">Whether a missing project is an error.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The project information or null.</returns>
        /// <exception cref="DurableCliException">Thrown when the project is required but missing, ambiguous or cannot be evaluated.</exception>
        public async Task<ProjectInfo?> GetProjectAsync(bool required, CancellationToken token)
        {
            if (_Project != null) return _Project;
            if (Settings.AssemblyPath != null) return null;
            string? file = ProjectBuilder.Locate(Settings.ProjectPath, Context, required);
            if (file == null) return null;
            _Project = await ProjectBuilder.InspectAsync(file, Settings.Configuration, Settings.Framework, Context, token).ConfigureAwait(false);
            return _Project;
        }

        /// <summary>
        /// Loads the user's assembly: the <c>--assembly</c> file, or the project's output after building it (unless <c>--no-build</c>).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The loaded assembly.</returns>
        /// <exception cref="DurableCliException">Thrown when the project cannot be found or built or the assembly cannot be loaded.</exception>
        public async Task<UserAssembly> LoadAssemblyAsync(CancellationToken token)
        {
            if (_Assembly != null) return _Assembly;
            string path;
            if (Settings.AssemblyPath != null)
            {
                path = Settings.AssemblyPath;
            }
            else
            {
                ProjectInfo project = (await GetProjectAsync(true, token).ConfigureAwait(false))!;
                if (!Settings.NoBuild) await ProjectBuilder.BuildAsync(project, Settings.Configuration, Context, Settings.Verbose, token).ConfigureAwait(false);
                path = project.TargetPath;
            }

            UserAssembly assembly = UserAssembly.Load(path);
            foreach (string warning in assembly.Warnings) Error.WriteLine("warning: " + warning);
            _Assembly = assembly;
            return assembly;
        }

        /// <summary>
        /// Loads the user's migrations, honoring --migrations-namespace.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The migrations, ordered by Id.</returns>
        /// <exception cref="DurableCliException">Thrown when the assembly cannot be loaded.</exception>
        public async Task<List<Migration>> LoadMigrationsAsync(CancellationToken token)
        {
            UserAssembly assembly = await LoadAssemblyAsync(token).ConfigureAwait(false);
            List<Migration> migrations = assembly.DiscoverMigrations(Settings.MigrationsNamespace);
            if (migrations.Count == 0)
            {
                Error.WriteLine("warning: no migrations found in " + Path.GetFileName(assembly.Path) +
                    (Settings.MigrationsNamespace != null ? " (namespace " + Settings.MigrationsNamespace + ")" : string.Empty) +
                    ". Migrations are concrete Migration subclasses with a public parameterless constructor.");
            }

            return migrations;
        }

        /// <summary>
        /// Loads the user's entity types, honoring --entities, --entities-namespace and --mapping-source. With a mapping source,
        /// the source is registered (<see cref="DurableMapping.Register(Type, IEntityMappingSource)"/>) for every type it
        /// describes before any metadata is built.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The entity types (never empty).</returns>
        /// <exception cref="DurableCliException">Thrown when no entity types are found or the mapping source cannot be used.</exception>
        public async Task<List<Type>> LoadEntitiesAsync(CancellationToken token)
        {
            UserAssembly assembly = await LoadAssemblyAsync(token).ConfigureAwait(false);
            IEntityMappingSource? mappingSource = Settings.MappingSource != null ? assembly.CreateMappingSource(Settings.MappingSource) : null;
            List<Type> entities = assembly.DiscoverEntities(Settings.Entities, Settings.EntitiesNamespace, mappingSource);
            if (mappingSource != null)
            {
                foreach (Type entity in entities.Where(mappingSource.Describes))
                {
                    try
                    {
                        DurableMapping.Register(entity, mappingSource);
                    }
                    catch (InvalidOperationException e)
                    {
                        throw new DurableCliException(e.Message);
                    }
                }
            }

            if (entities.Count == 0)
            {
                throw new DurableCliException("No entity types found in " + Path.GetFileName(assembly.Path) +
                    (Settings.EntitiesNamespace != null ? " (namespace " + Settings.EntitiesNamespace + ")" : string.Empty) +
                    ". Entity types are classes with [Entity(\"table\")] or described by --mapping-source; or list them with --entities.");
            }

            if (Settings.Verbose) Error.WriteLine("Entity types: " + string.Join(", ", entities.Select(t => t.FullName)));
            return entities;
        }

        /// <summary>
        /// Creates a migrator for the database with the given migrations and the configured history table.
        /// </summary>
        /// <param name="database">Database. Must not be null.</param>
        /// <param name="migrations">Migrations. Must not be null.</param>
        /// <returns>The migrator.</returns>
        /// <exception cref="DurableCliException">Thrown when the history table name is invalid.</exception>
        public SqlMigrator CreateMigrator(DatabaseTarget database, IEnumerable<Migration> migrations)
        {
            ArgumentNullException.ThrowIfNull(database);
            SqlMigratorOptions options = new SqlMigratorOptions();
            if (Settings.HistoryTable != null)
            {
                try
                {
                    options.HistoryTableName = Settings.HistoryTable;
                }
                catch (ArgumentException e)
                {
                    throw new DurableCliException("Invalid --history-table '" + Settings.HistoryTable + "': " + e.Message);
                }
            }

            if (Settings.Verbose) options.StatementExecuted = statement => Error.WriteLine("  sql> " + statement.Sql);
            return new SqlMigrator(database.ConnectionFactory, database.Dialect, migrations, options);
        }

        /// <summary>
        /// Gets the effective history table name.
        /// </summary>
        public string HistoryTableName => Settings.HistoryTable ?? new SqlMigratorOptions().HistoryTableName;

        /// <summary>
        /// Returns the project whose directory and root namespace generated files use: the configured or discovered project,
        /// or null when <c>--assembly</c> is used or the working directory has no project (generated files then go relative
        /// to the working directory).
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The project or null.</returns>
        public async Task<ProjectInfo?> TryGetProjectForOutputAsync(CancellationToken token)
        {
            if (Settings.AssemblyPath != null) return null;
            return await GetProjectAsync(Settings.ProjectPath != null, token).ConfigureAwait(false);
        }

        private async Task AddProjectOutputProbeAsync(CancellationToken token)
        {
            if (Settings.AssemblyPath != null)
            {
                if (File.Exists(Settings.AssemblyPath)) DuckDbNativeLibrary.AddAssembly(Settings.AssemblyPath);
                return;
            }

            try
            {
                ProjectInfo? project = await GetProjectAsync(false, token).ConfigureAwait(false);
                if (project != null && File.Exists(project.TargetPath)) DuckDbNativeLibrary.AddAssembly(project.TargetPath);
            }
            catch (DurableCliException)
            {
                // No usable project: the environment variable and the default probing still apply.
            }
        }
    }
}
