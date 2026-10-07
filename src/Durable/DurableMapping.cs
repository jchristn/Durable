namespace Durable
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Text;

    /// <summary>
    /// Global mapping conventions used when an entity is not fully described by attributes.
    /// An entity without an <see cref="EntityAttribute"/> uses its class name (transformed by <see cref="NamingConvention"/>) as the table name.
    /// An entity without any <see cref="PropertyAttribute"/> maps every public read/write property that is a scalar
    /// (not a navigation and not marked <see cref="NotMappedAttribute"/>), and uses <see cref="KeyPropertyNames"/> to find the primary key.
    /// Classes that cannot carry Durable's attributes can be mapped by an <see cref="IEntityMappingSource"/>, registered
    /// per type with <see cref="Register{T}(IEntityMappingSource)"/> or for every type with <see cref="MappingSource"/>.
    /// Configure these values once at startup, before the first repository for an entity is created: metadata is cached per type.
    /// Thread safety: the convention setters are not synchronized with metadata construction; do not change them while repositories
    /// are in use. Mapping-source registration is thread-safe but, like the conventions, meant for startup.
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

        /// <summary>
        /// Gets or sets the mapping source consulted for every type that has no source registered with
        /// <see cref="Register{T}(IEntityMappingSource)"/>. Durable uses it for a type only when its
        /// <see cref="IEntityMappingSource.Describes(Type)"/> returns true, and reads the type's own attributes otherwise.
        /// Default: null (attributes and conventions only).
        /// Types whose metadata was built before the change keep the mapping they were built with; setting the same source
        /// again is allowed.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the new source describes a type whose metadata has already been built from a different source.
        /// </exception>
        public static IEntityMappingSource? MappingSource
        {
            get => _MappingSource;
            set
            {
                lock (_RegistrationLock)
                {
                    if (value != null)
                    {
                        foreach (EntityMetadata built in EntityMetadata.AllBuilt())
                        {
                            if (_Registrations.ContainsKey(built.EntityType) || Equals(built.MappingSource, value)) continue;
                            if (value.Describes(built.EntityType)) throw AlreadyBuilt(built.EntityType);
                        }
                    }

                    _MappingSource = value;
                }
            }
        }

        /// <summary>
        /// Gets the built-in mapping source, which reads Durable's attributes from the entity class.
        /// Useful inside a custom <see cref="IEntityMappingSource"/> that adds to, rather than replaces, a class's own attributes.
        /// Never null. Thread safety: stateless; safe to call concurrently.
        /// </summary>
        public static IEntityMappingSource AttributeSource => AttributeMappingSource.Instance;

        #endregion

        #region Private-Members

        private static IReadOnlyList<string> _KeyPropertyNames = new List<string> { "Id", "{Type}Id" };
        private static readonly object _RegistrationLock = new object();
        private static readonly ConcurrentDictionary<Type, IEntityMappingSource> _Registrations = new ConcurrentDictionary<Type, IEntityMappingSource>();
        private static volatile IEntityMappingSource? _MappingSource;

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

        /// <summary>
        /// Registers the mapping source for one entity type. A registered source takes precedence over
        /// <see cref="MappingSource"/> and over the type's own attributes, and is used whether or not its
        /// <see cref="IEntityMappingSource.Describes(Type)"/> returns true. Registering again for the same type replaces the
        /// earlier source, as long as the type's metadata has not been built; registering the source a built type already
        /// uses does nothing.
        /// Call at startup, before the first repository, query or <see cref="EntityMetadata.For{T}"/> call for the type.
        /// Thread safety: thread-safe.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="source">Mapping source. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when source is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the type's metadata has already been built from a different source.</exception>
        public static void Register<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(IEntityMappingSource source)
        {
            Register(typeof(T), source);
        }

        /// <summary>
        /// Registers the mapping source for one entity type. See <see cref="Register{T}(IEntityMappingSource)"/>.
        /// Thread safety: thread-safe.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="source">Mapping source. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType or source is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the type's metadata has already been built from a different source.</exception>
        public static void Register([DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] Type entityType, IEntityMappingSource source)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(source);
            lock (_RegistrationLock)
            {
                EntityMetadata? built = EntityMetadata.TryGetBuilt(entityType);
                if (built != null)
                {
                    if (Equals(built.MappingSource, source)) return;
                    throw AlreadyBuilt(entityType);
                }

                _Registrations[entityType] = source;
            }
        }

        /// <summary>
        /// Gets the mapping source Durable uses for a type: its registered source, else <see cref="MappingSource"/> when that
        /// source describes the type, else <see cref="AttributeSource"/>.
        /// Thread safety: thread-safe.
        /// </summary>
        /// <param name="entityType">Entity or projection type. Must not be null.</param>
        /// <returns>The mapping source. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        public static IEntityMappingSource GetMappingSource(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            if (_Registrations.TryGetValue(entityType, out IEntityMappingSource? registered)) return registered;
            IEntityMappingSource? global = _MappingSource;
            if (global != null && global.Describes(entityType)) return global;
            return AttributeMappingSource.Instance;
        }

        #endregion

        #region Private-Methods

        private static InvalidOperationException AlreadyBuilt(Type entityType)
        {
            return new InvalidOperationException(
                "The mapping of " + entityType.FullName + " cannot change because its metadata has already been built. " +
                "Register mapping sources at startup, before the first repository, query or EntityMetadata.For call for the type.");
        }

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
