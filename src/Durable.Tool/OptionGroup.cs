namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// A titled group of options, used to organize command help.
    /// </summary>
    internal sealed class OptionGroup
    {
        /// <summary>
        /// Gets the heading shown in help.
        /// </summary>
        public string Title { get; }

        /// <summary>
        /// Gets the options in display order.
        /// </summary>
        public IReadOnlyList<OptionDefinition> Options { get; }

        /// <summary>
        /// Instantiates a group.
        /// </summary>
        /// <param name="title">Heading. Must not be null.</param>
        /// <param name="options">Options. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public OptionGroup(string title, params OptionDefinition[] options)
        {
            Title = title ?? throw new ArgumentNullException(nameof(title));
            ArgumentNullException.ThrowIfNull(options);
            Options = options.ToList();
        }
    }
}
