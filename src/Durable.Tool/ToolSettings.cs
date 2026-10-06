namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.Json;

    /// <summary>
    /// Settings shared by the commands after merging, in order of precedence, the command line, the environment
    /// (DURABLE_PROVIDER, DURABLE_CONNECTION) and <c>durable.json</c>. Paths are absolute.
    /// </summary>
    internal sealed class ToolSettings
    {
        /// <summary>Gets the provider name, or null.</summary>
        public string? Provider { get; private set; }

        /// <summary>Gets the connection string, or null.</summary>
        public string? Connection { get; private set; }

        /// <summary>Gets the assembly path, or null.</summary>
        public string? AssemblyPath { get; private set; }

        /// <summary>Gets the project file or directory path, or null.</summary>
        public string? ProjectPath { get; private set; }

        /// <summary>Gets the target framework, or null.</summary>
        public string? Framework { get; private set; }

        /// <summary>Gets the build configuration. Default: Debug.</summary>
        public string Configuration { get; private set; } = "Debug";

        /// <summary>Gets whether to skip building the project.</summary>
        public bool NoBuild { get; private set; }

        /// <summary>Gets the migration discovery namespace, or null for all.</summary>
        public string? MigrationsNamespace { get; private set; }

        /// <summary>Gets the entity discovery namespace, or null for all.</summary>
        public string? EntitiesNamespace { get; private set; }

        /// <summary>Gets the explicit entity type names; empty to discover [Entity] types.</summary>
        public List<string> Entities { get; private set; } = new List<string>();

        /// <summary>Gets the migration history table, or null for the default.</summary>
        public string? HistoryTable { get; private set; }

        /// <summary>Gets whether verbose output is enabled.</summary>
        public bool Verbose { get; private set; }

        /// <summary>Gets the settings file that was read, or null.</summary>
        public string? ConfigPath { get; private set; }

        /// <summary>
        /// Resolves the settings for a command.
        /// </summary>
        /// <param name="arguments">Parsed command arguments. Must not be null.</param>
        /// <param name="context">Invocation context. Must not be null.</param>
        /// <returns>The settings.</returns>
        /// <exception cref="DurableCliException">Thrown when the settings file is missing or invalid.</exception>
        public static ToolSettings Resolve(ParsedArguments arguments, DurableCliContext context)
        {
            ArgumentNullException.ThrowIfNull(arguments);
            ArgumentNullException.ThrowIfNull(context);
            ToolSettings settings = new ToolSettings();

            string? explicitConfig = arguments.GetValue(CliOptions.Config);
            string configPath = explicitConfig != null ? context.ResolvePath(explicitConfig) : Path.Combine(context.WorkingDirectory, "durable.json");
            DurableJsonFile file = new DurableJsonFile();
            string configDirectory = context.WorkingDirectory;
            if (File.Exists(configPath))
            {
                file = ReadFile(configPath);
                settings.ConfigPath = configPath;
                configDirectory = Path.GetDirectoryName(configPath) ?? context.WorkingDirectory;
            }
            else if (explicitConfig != null)
            {
                throw new DurableCliException("Settings file '" + configPath + "' was not found.");
            }

            settings.Provider = Pick(arguments.GetValue(CliOptions.Provider), context.GetEnvironmentVariable("DURABLE_PROVIDER"), file.Provider);
            settings.Connection = Pick(arguments.GetValue(CliOptions.Connection), context.GetEnvironmentVariable("DURABLE_CONNECTION"), file.Connection);
            settings.AssemblyPath = PickPath(arguments.GetValue(CliOptions.Assembly), file.Assembly, context, configDirectory);
            settings.ProjectPath = PickPath(arguments.GetValue(CliOptions.Project), file.Project, context, configDirectory);
            settings.Framework = Pick(arguments.GetValue(CliOptions.Framework), null, file.Framework);
            settings.Configuration = Pick(arguments.GetValue(CliOptions.Configuration), null, file.Configuration) ?? "Debug";
            settings.NoBuild = arguments.HasFlag(CliOptions.NoBuild);
            settings.MigrationsNamespace = Pick(arguments.GetValue(CliOptions.MigrationsNamespace), null, file.MigrationsNamespace);
            settings.EntitiesNamespace = Pick(arguments.GetValue(CliOptions.EntitiesNamespace), null, file.EntitiesNamespace);
            settings.HistoryTable = Pick(arguments.GetValue(CliOptions.HistoryTable), null, file.HistoryTable);
            settings.Verbose = arguments.HasFlag(CliOptions.Verbose);

            string? entities = arguments.GetValue(CliOptions.Entities);
            if (entities != null) settings.Entities = SplitList(entities);
            else if (file.Entities != null) settings.Entities = file.Entities.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e.Trim()).ToList();

            bool cliAssembly = arguments.GetValue(CliOptions.Assembly) != null;
            bool cliProject = arguments.GetValue(CliOptions.Project) != null;
            if (cliAssembly && cliProject) throw new DurableCliException("Pass either --assembly or --project, not both.", null, true);
            if (cliAssembly) settings.ProjectPath = null;
            if (cliProject) settings.AssemblyPath = null;
            return settings;
        }

        /// <summary>
        /// Splits a comma-separated list, trimming entries and dropping empty ones.
        /// </summary>
        /// <param name="value">List text. Must not be null.</param>
        /// <returns>The entries.</returns>
        public static List<string> SplitList(string value)
        {
            ArgumentNullException.ThrowIfNull(value);
            return value.Split(',').Select(v => v.Trim()).Where(v => v.Length > 0).ToList();
        }

        private static DurableJsonFile ReadFile(string path)
        {
            JsonSerializerOptions options = new JsonSerializerOptions
            {
                ReadCommentHandling = JsonCommentHandling.Skip,
                AllowTrailingCommas = true,
                UnmappedMemberHandling = System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow
            };

            try
            {
                return JsonSerializer.Deserialize<DurableJsonFile>(File.ReadAllText(path), options) ?? new DurableJsonFile();
            }
            catch (JsonException e)
            {
                throw new DurableCliException("Settings file '" + path + "' is invalid: " + e.Message +
                    " Supported properties: provider, connection, project, assembly, framework, configuration, migrationsNamespace, entitiesNamespace, entities, historyTable.");
            }
            catch (IOException e)
            {
                throw new DurableCliException("Settings file '" + path + "' could not be read: " + e.Message);
            }
        }

        private static string? Pick(string? commandLine, string? environment, string? file)
        {
            if (!string.IsNullOrWhiteSpace(commandLine)) return commandLine;
            if (!string.IsNullOrWhiteSpace(environment)) return environment;
            if (!string.IsNullOrWhiteSpace(file)) return file;
            return null;
        }

        private static string? PickPath(string? commandLine, string? file, DurableCliContext context, string configDirectory)
        {
            if (!string.IsNullOrWhiteSpace(commandLine)) return context.ResolvePath(commandLine);
            if (!string.IsNullOrWhiteSpace(file)) return Path.GetFullPath(Path.Combine(configDirectory, file));
            return null;
        }
    }
}
