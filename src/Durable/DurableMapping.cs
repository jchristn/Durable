namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Global mapping conventions used when an entity is not fully described by attributes.
    /// An entity without an <see cref="EntityAttribute"/> uses its class name (transformed by <see cref="NamingConvention"/>) as the table name.
    /// An entity without any <see cref="PropertyAttribute"/> maps every public read/write property that is a scalar
    /// (not a navigation and not marked <see cref="NotMappedAttribute"/>), and uses <see cref="KeyPropertyNames"/> to find the primary key.
    /// Configure these values once at startup, before the first repository for an entity is created: metadata is cached per type.
    /// Thread safety: the setters are not synchronized with metadata construction; do not change them while repositories are in use.
    /// </summary>
    public static class DurableMapping
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the naming convention applied to convention-mapped table and column names. Default: <see cref="NamingConvention.AsIs"/>.
        /// </summary>
        public static NamingConvention NamingConvention { get; set; } = NamingConvention.AsIs;

        /// <summary>
        /// Gets or sets the property names recognized as the primary key for convention-mapped entities, matched case-insensitively.
        /// The token "{Type}" is replaced with the entity class name. Default: "Id", "{Type}Id".
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when set to null.</exception>
        public static IReadOnlyList<string> KeyPropertyNames
        {
            get => _KeyPropertyNames;
            set => _KeyPropertyNames = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Gets or sets whether a convention-mapped integer primary key is treated as database-generated. Default: true.
        /// </summary>
        public static bool ConventionKeysAreAutoIncrement { get; set; } = true;

        #endregion

        #region Private-Members

        private static IReadOnlyList<string> _KeyPropertyNames = new List<string> { "Id", "{Type}Id" };

        #endregion

        #region Public-Methods

        /// <summary>
        /// Applies the configured <see cref="NamingConvention"/> to a name.
        /// </summary>
        /// <param name="name">Name to transform. Must not be null.</param>
        /// <returns>The transformed name.</returns>
        /// <exception cref="ArgumentNullException">Thrown when name is null.</exception>
        public static string ApplyNamingConvention(string name)
        {
            ArgumentNullException.ThrowIfNull(name);
            switch (NamingConvention)
            {
                case NamingConvention.SnakeCase:
                    return ToSnakeCase(name);
                case NamingConvention.Lowercase:
                    return name.ToLowerInvariant();
                default:
                    return name;
            }
        }

        #endregion

        #region Private-Methods

        private static string ToSnakeCase(string name)
        {
            StringBuilder sb = new StringBuilder(name.Length + 8);
            for (int i = 0; i < name.Length; i++)
            {
                char c = name[i];
                if (char.IsUpper(c))
                {
                    bool previousIsLowerOrDigit = i > 0 && (char.IsLower(name[i - 1]) || char.IsDigit(name[i - 1]));
                    bool nextIsLower = i + 1 < name.Length && char.IsLower(name[i + 1]);
                    bool previousIsUpper = i > 0 && char.IsUpper(name[i - 1]);
                    if (i > 0 && (previousIsLowerOrDigit || (previousIsUpper && nextIsLower)) && name[i - 1] != '_')
                        sb.Append('_');
                    sb.Append(char.ToLowerInvariant(c));
                }
                else
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }

        #endregion
    }
}
