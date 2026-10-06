namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A command: its name (one or two words), help text, accepted options and handler.
    /// </summary>
    internal sealed class CommandDefinition
    {
        /// <summary>
        /// Gets the command words, for example "migrations add".
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the number of words in <see cref="Name"/>.
        /// </summary>
        public int WordCount => Name.Split(' ').Length;

        /// <summary>
        /// Gets the help category ("Migrations" or "Schema").
        /// </summary>
        public string Category { get; }

        /// <summary>
        /// Gets the one-line summary.
        /// </summary>
        public string Summary { get; }

        /// <summary>
        /// Gets the longer help description (may contain line breaks).
        /// </summary>
        public string Description { get; }

        /// <summary>
        /// Gets the positional argument syntax, for example "&lt;Name&gt;"; empty when the command takes none.
        /// </summary>
        public string ArgumentSyntax { get; }

        /// <summary>
        /// Gets the help text of the positional argument; null when the command takes none.
        /// </summary>
        public string? ArgumentDescription { get; }

        /// <summary>
        /// Gets the number of required positional arguments (the command accepts exactly this many).
        /// </summary>
        public int ArgumentCount { get; }

        /// <summary>
        /// Gets the option groups shown in help, in order.
        /// </summary>
        public IReadOnlyList<OptionGroup> OptionGroups { get; }

        /// <summary>
        /// Gets the usage examples.
        /// </summary>
        public IReadOnlyList<string> Examples { get; }

        /// <summary>
        /// Gets another name for the same command shown in help, or null.
        /// </summary>
        public string? AliasOf { get; }

        /// <summary>
        /// Gets the handler.
        /// </summary>
        public Func<CommandInvocation, CancellationToken, Task<int>> Handler { get; }

        /// <summary>
        /// Gets every option the command accepts, including the general options.
        /// </summary>
        public IReadOnlyList<OptionDefinition> AllOptions => OptionGroups.SelectMany(g => g.Options).ToList();

        /// <summary>
        /// Instantiates a command.
        /// </summary>
        /// <param name="name">Command words. Must not be null or empty.</param>
        /// <param name="category">Help category. Must not be null.</param>
        /// <param name="summary">One-line summary. Must not be null.</param>
        /// <param name="description">Help description. Must not be null.</param>
        /// <param name="argumentSyntax">Positional argument syntax; empty for none. Must not be null.</param>
        /// <param name="argumentDescription">Positional argument help; null for none.</param>
        /// <param name="argumentCount">Required positional argument count. Minimum: 0.</param>
        /// <param name="optionGroups">Option groups. Must not be null.</param>
        /// <param name="examples">Examples. Must not be null.</param>
        /// <param name="handler">Handler. Must not be null.</param>
        /// <param name="aliasOf">Name of the command this one aliases, or null.</param>
        /// <exception cref="ArgumentException">Thrown when name is null or empty or argumentCount is negative.</exception>
        /// <exception cref="ArgumentNullException">Thrown when a required argument is null.</exception>
        public CommandDefinition(
            string name,
            string category,
            string summary,
            string description,
            string argumentSyntax,
            string? argumentDescription,
            int argumentCount,
            IEnumerable<OptionGroup> optionGroups,
            IEnumerable<string> examples,
            Func<CommandInvocation, CancellationToken, Task<int>> handler,
            string? aliasOf = null)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Command name cannot be null or empty.", nameof(name));
            if (argumentCount < 0) throw new ArgumentException("Argument count cannot be negative.", nameof(argumentCount));
            ArgumentNullException.ThrowIfNull(optionGroups);
            ArgumentNullException.ThrowIfNull(examples);
            Name = name;
            Category = category ?? throw new ArgumentNullException(nameof(category));
            Summary = summary ?? throw new ArgumentNullException(nameof(summary));
            Description = description ?? throw new ArgumentNullException(nameof(description));
            ArgumentSyntax = argumentSyntax ?? throw new ArgumentNullException(nameof(argumentSyntax));
            ArgumentDescription = argumentDescription;
            ArgumentCount = argumentCount;
            OptionGroups = optionGroups.ToList();
            Examples = examples.ToList();
            Handler = handler ?? throw new ArgumentNullException(nameof(handler));
            AliasOf = aliasOf;
        }
    }
}
