namespace Durable.Tool
{
    using System.Collections.Generic;

    /// <summary>
    /// Every option the tool understands, shared by the command definitions.
    /// </summary>
    internal static class CliOptions
    {
        /// <summary>Database provider.</summary>
        public static readonly OptionDefinition Provider = new OptionDefinition("provider", "name", "Database provider: sqlite, postgres, mysql or sqlserver (env DURABLE_PROVIDER)");

        /// <summary>Connection string.</summary>
        public static readonly OptionDefinition Connection = new OptionDefinition("connection", "string", "Connection string (env DURABLE_CONNECTION)");

        /// <summary>Migration history table.</summary>
        public static readonly OptionDefinition HistoryTable = new OptionDefinition("history-table", "name", "Migration history table (default: __durable_migrations)");

        /// <summary>Built assembly.</summary>
        public static readonly OptionDefinition Assembly = new OptionDefinition("assembly", "path", "Built assembly (.dll) containing migrations/entities; skips building");

        /// <summary>Project to build.</summary>
        public static readonly OptionDefinition Project = new OptionDefinition("project", "path", "Project file or directory to build (default: the single .csproj in the current directory)");

        /// <summary>Target framework.</summary>
        public static readonly OptionDefinition Framework = new OptionDefinition("framework", "tfm", "Target framework of a multi-targeted project, for example net8.0");

        /// <summary>Build configuration.</summary>
        public static readonly OptionDefinition Configuration = new OptionDefinition("configuration", "name", "Build configuration (default: Debug)");

        /// <summary>Skip the build.</summary>
        public static readonly OptionDefinition NoBuild = new OptionDefinition("no-build", null, "Use the project's existing build output instead of building it");

        /// <summary>Migration discovery scope.</summary>
        public static readonly OptionDefinition MigrationsNamespace = new OptionDefinition("migrations-namespace", "ns", "Only use migrations in this namespace (and nested namespaces)");

        /// <summary>Entity discovery scope.</summary>
        public static readonly OptionDefinition EntitiesNamespace = new OptionDefinition("entities-namespace", "ns", "Only use [Entity] types in this namespace (and nested namespaces)");

        /// <summary>Explicit entity list.</summary>
        public static readonly OptionDefinition Entities = new OptionDefinition("entities", "types", "Comma-separated entity type names (full or simple) instead of discovering [Entity] types");

        /// <summary>Configuration file.</summary>
        public static readonly OptionDefinition Config = new OptionDefinition("config", "path", "Settings file (default: ./durable.json when present)");

        /// <summary>Verbose output.</summary>
        public static readonly OptionDefinition Verbose = new OptionDefinition("verbose", null, "Show build output, executed SQL and stack traces");

        /// <summary>Help.</summary>
        public static readonly OptionDefinition Help = new OptionDefinition("help", null, "Show help", 'h');

        /// <summary>Migration target.</summary>
        public static readonly OptionDefinition MigrateTarget = new OptionDefinition("target", "id", "Apply migrations up to and including this migration Id (default: all)");

        /// <summary>Rollback target.</summary>
        public static readonly OptionDefinition RollbackTarget = new OptionDefinition("target", "id|0", "Keep migrations up to and including this Id and revert later ones; 0 reverts all (required)");

        /// <summary>Script lower bound.</summary>
        public static readonly OptionDefinition From = new OptionDefinition("from", "id|0", "Start after this migration Id (exclusive; 0 = from the beginning). Scripts the range regardless of history");

        /// <summary>Script upper bound.</summary>
        public static readonly OptionDefinition To = new OptionDefinition("to", "id", "End with this migration Id (inclusive; default: the last migration)");

        /// <summary>Output file.</summary>
        public static readonly OptionDefinition Output = new OptionDefinition("output", "file", "Write the script to this file instead of standard output");

        /// <summary>Generated code directory for migrations.</summary>
        public static readonly OptionDefinition MigrationsOutputDir = new OptionDefinition("output-dir", "dir", "Directory for the new file, relative to the project directory (default: Migrations)");

        /// <summary>Generated code directory for entities.</summary>
        public static readonly OptionDefinition EntitiesOutputDir = new OptionDefinition("output-dir", "dir", "Directory for the generated files, relative to the project directory (default: Entities)");

        /// <summary>Generated code namespace.</summary>
        public static readonly OptionDefinition Namespace = new OptionDefinition("namespace", "ns", "Namespace of the generated code (default: root namespace + output directory)");

        /// <summary>Empty migration template.</summary>
        public static readonly OptionDefinition Empty = new OptionDefinition("empty", null, "Create an empty migration even when a database connection is configured");

        /// <summary>Allow destructive operations.</summary>
        public static readonly OptionDefinition AllowDestructive = new OptionDefinition("allow-destructive", null, "Include destructive operations (drop unmapped columns and indexes)");

        /// <summary>Dry run.</summary>
        public static readonly OptionDefinition DryRun = new OptionDefinition("dry-run", null, "Print the SQL that would run instead of changing the database");

        /// <summary>Print the diff as SQL.</summary>
        public static readonly OptionDefinition Sql = new OptionDefinition("sql", null, "Print the differences as a reviewable SQL script");

        /// <summary>Tables to scaffold.</summary>
        public static readonly OptionDefinition Tables = new OptionDefinition("tables", "names", "Comma-separated tables to scaffold (default: all tables except the history table)");

        /// <summary>Overwrite files.</summary>
        public static readonly OptionDefinition Force = new OptionDefinition("force", null, "Overwrite existing files");

        /// <summary>Keep table names as class names.</summary>
        public static readonly OptionDefinition NoSingularize = new OptionDefinition("no-singularize", null, "Do not singularize table names when naming classes");

        /// <summary>
        /// Gets every option, used to find command words in argument lists before the command is known.
        /// </summary>
        public static IReadOnlyList<OptionDefinition> All { get; } = new List<OptionDefinition>
        {
            Provider, Connection, HistoryTable, Assembly, Project, Framework, Configuration, NoBuild, MigrationsNamespace,
            EntitiesNamespace, Entities, Config, Verbose, Help, MigrateTarget, From, To, Output, MigrationsOutputDir, Namespace,
            Empty, AllowDestructive, DryRun, Sql, Tables, Force, NoSingularize
        };
    }
}
