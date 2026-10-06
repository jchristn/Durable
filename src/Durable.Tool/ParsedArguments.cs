namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// The arguments of one command after parsing: positional arguments (excluding the command words), option values and flags.
    /// </summary>
    internal sealed class ParsedArguments
    {
        /// <summary>
        /// Gets the positional arguments that follow the command words.
        /// </summary>
        public List<string> Positionals { get; } = new List<string>();

        private readonly Dictionary<string, string> _Values = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly HashSet<string> _Flags = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>
        /// Returns the value given for an option, or null when it was not given.
        /// </summary>
        /// <param name="option">Option. Must not be null.</param>
        /// <returns>The value or null.</returns>
        public string? GetValue(OptionDefinition option)
        {
            ArgumentNullException.ThrowIfNull(option);
            return _Values.TryGetValue(option.Name, out string? value) ? value : null;
        }

        /// <summary>
        /// Returns whether a flag was given.
        /// </summary>
        /// <param name="option">Flag option. Must not be null.</param>
        /// <returns>True when given.</returns>
        public bool HasFlag(OptionDefinition option)
        {
            ArgumentNullException.ThrowIfNull(option);
            return _Flags.Contains(option.Name);
        }

        /// <summary>
        /// Records an option value.
        /// </summary>
        /// <param name="option">Option. Must not be null.</param>
        /// <param name="value">Value. Must not be null.</param>
        /// <exception cref="DurableCliException">Thrown when the option was already given.</exception>
        public void SetValue(OptionDefinition option, string value)
        {
            if (!_Values.TryAdd(option.Name, value))
                throw new DurableCliException("Option --" + option.Name + " was given more than once.", null, true);
        }

        /// <summary>
        /// Records a flag.
        /// </summary>
        /// <param name="option">Flag option. Must not be null.</param>
        public void SetFlag(OptionDefinition option)
        {
            _Flags.Add(option.Name);
        }
    }
}
