namespace Durable.Tool
{
    /// <summary>
    /// The option groups shared by several commands.
    /// </summary>
    internal static class CommandOptionGroups
    {
        /// <summary>
        /// Database options with the migration history table.
        /// </summary>
        public static OptionGroup DatabaseWithHistory => new OptionGroup("Database", CliOptions.Provider, CliOptions.Connection, CliOptions.HistoryTable);

        /// <summary>
        /// Database options.
        /// </summary>
        public static OptionGroup Database => new OptionGroup("Database", CliOptions.Provider, CliOptions.Connection);

        /// <summary>
        /// Options locating the user's code with migration discovery.
        /// </summary>
        public static OptionGroup ProjectWithMigrations => new OptionGroup(
            "Project", CliOptions.Project, CliOptions.Assembly, CliOptions.Framework, CliOptions.Configuration, CliOptions.NoBuild, CliOptions.MigrationsNamespace);

        /// <summary>
        /// Options locating the user's code with entity discovery.
        /// </summary>
        public static OptionGroup ProjectWithEntities => new OptionGroup(
            "Project", CliOptions.Project, CliOptions.Assembly, CliOptions.Framework, CliOptions.Configuration, CliOptions.NoBuild, CliOptions.EntitiesNamespace, CliOptions.Entities);

        /// <summary>
        /// Options locating the user's project (no discovery), used by scaffold for the output directory and namespace.
        /// </summary>
        public static OptionGroup ProjectForOutput => new OptionGroup("Project", CliOptions.Project, CliOptions.Framework, CliOptions.Configuration);

        /// <summary>
        /// General options accepted by every command.
        /// </summary>
        public static OptionGroup General => new OptionGroup("General", CliOptions.Config, CliOptions.Verbose, CliOptions.Help);
    }
}
