namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The commands of the tool and lookup by command words.
    /// </summary>
    internal static class CommandCatalog
    {
        /// <summary>
        /// Gets every command in help order.
        /// </summary>
        public static IReadOnlyList<CommandDefinition> Commands { get; } = new List<CommandDefinition>
        {
            MigrateCommand.Definition,
            RollbackCommand.Definition,
            StatusCommand.Definition,
            ScriptCommand.Definition,
            MigrationsAddCommand.Definition,
            StatusCommand.ListDefinition,
            SchemaDiffCommand.Definition,
            SchemaSyncCommand.Definition,
            ScaffoldCommand.Definition
        };

        /// <summary>
        /// Gets the command group words that have subcommands ("migrations", "schema").
        /// </summary>
        public static IReadOnlyList<string> Groups { get; } = Commands.Where(c => c.WordCount > 1).Select(c => c.Name.Split(' ')[0]).Distinct().ToList();

        /// <summary>
        /// Finds the command named by the leading words, preferring two-word commands.
        /// </summary>
        /// <param name="words">Non-option words of the command line. Must not be null.</param>
        /// <returns>The command, or null when none matches.</returns>
        public static CommandDefinition? Find(IReadOnlyList<string> words)
        {
            ArgumentNullException.ThrowIfNull(words);
            if (words.Count >= 2)
            {
                string two = words[0] + " " + words[1];
                CommandDefinition? match = Commands.FirstOrDefault(c => string.Equals(c.Name, two, StringComparison.OrdinalIgnoreCase));
                if (match != null) return match;
            }

            if (words.Count >= 1) return Commands.FirstOrDefault(c => c.WordCount == 1 && string.Equals(c.Name, words[0], StringComparison.OrdinalIgnoreCase));
            return null;
        }

        /// <summary>
        /// Returns the subcommands of a group word.
        /// </summary>
        /// <param name="group">Group word. Must not be null.</param>
        /// <returns>The subcommands; empty when the word is not a group.</returns>
        public static List<CommandDefinition> InGroup(string group)
        {
            ArgumentNullException.ThrowIfNull(group);
            return Commands.Where(c => c.WordCount > 1 && string.Equals(c.Name.Split(' ')[0], group, StringComparison.OrdinalIgnoreCase)).ToList();
        }
    }
}
