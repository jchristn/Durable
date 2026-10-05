namespace Durable
{
    using System;

    /// <summary>
    /// Maps a property to a column.
    /// Properties without this attribute are still mapped by convention when the entity has no
    /// <see cref="PropertyAttribute"/> on any property; see <see cref="DurableMapping"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class PropertyAttribute : Attribute
    {
        /// <summary>
        /// Gets the column name. Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the column flags.
        /// </summary>
        public Flags PropertyFlags { get; }

        /// <summary>
        /// Gets the maximum length for string columns. Zero means unbounded / provider default.
        /// </summary>
        public int MaxLength { get; }

        /// <summary>
        /// Gets or sets the position of this column within a composite primary key.
        /// Lower values come first; ties are broken by declaration order. Default: 0.
        /// Ignored when the property is not part of the primary key.
        /// </summary>
        public int KeyOrder { get; set; } = 0;

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="name">Column name.</param>
        /// <param name="flags">Column flags.</param>
        /// <param name="maxLength">Maximum length for string columns; zero for no limit.</param>
        /// <exception cref="ArgumentNullException">Thrown when name is null or whitespace.</exception>
        public PropertyAttribute(string name, Flags flags = Flags.None, int maxLength = 0)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
            Name = name;
            PropertyFlags = flags;
            MaxLength = maxLength;
        }
    }
}
