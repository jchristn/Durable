namespace Durable.Tool
{
    using System;

    /// <summary>
    /// A command-line option: <c>--name value</c> (or <c>--name=value</c>) when <see cref="ValueName"/> is set, otherwise a flag.
    /// </summary>
    internal sealed class OptionDefinition
    {
        /// <summary>
        /// Gets the long name without the leading dashes, for example "provider".
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the placeholder shown in help for the value, for example "name"; null for a flag.
        /// </summary>
        public string? ValueName { get; }

        /// <summary>
        /// Gets the one-line help description.
        /// </summary>
        public string Description { get; }

        /// <summary>
        /// Gets the single-character alias without the dash, or null.
        /// </summary>
        public char? ShortName { get; }

        /// <summary>
        /// Gets whether the option is a flag (takes no value).
        /// </summary>
        public bool IsFlag => ValueName == null;

        /// <summary>
        /// Gets the syntax shown in help, for example "--provider &lt;name&gt;".
        /// </summary>
        public string Syntax => (ShortName != null ? "-" + ShortName + ", " : string.Empty) + "--" + Name + (ValueName != null ? " <" + ValueName + ">" : string.Empty);

        /// <summary>
        /// Instantiates an option.
        /// </summary>
        /// <param name="name">Long name without dashes. Must not be null or empty.</param>
        /// <param name="valueName">Value placeholder; null for a flag.</param>
        /// <param name="description">Help description. Must not be null.</param>
        /// <param name="shortName">Single-character alias, or null.</param>
        /// <exception cref="ArgumentException">Thrown when name is null or empty.</exception>
        /// <exception cref="ArgumentNullException">Thrown when description is null.</exception>
        public OptionDefinition(string name, string? valueName, string description, char? shortName = null)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Option name cannot be null or empty.", nameof(name));
            Name = name;
            ValueName = valueName;
            Description = description ?? throw new ArgumentNullException(nameof(description));
            ShortName = shortName;
        }
    }
}
