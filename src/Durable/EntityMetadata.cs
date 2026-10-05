namespace Durable
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Cached, immutable mapping description for an entity (or projection) type.
    /// Built once per type on first use from attributes, falling back to <see cref="DurableMapping"/> conventions.
    /// Thread safety: <see cref="For(Type)"/> is thread-safe; instances are immutable and safe to share.
    /// </summary>
    public sealed class EntityMetadata
    {
        #region Public-Members

        /// <summary>
        /// Gets the described CLR type. Never null.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Gets the table (or collection) name. Never null.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets whether the type carries an <see cref="EntityAttribute"/>.
        /// </summary>
        public bool HasEntityAttribute { get; }

        /// <summary>
        /// Gets whether columns were discovered by convention rather than <see cref="PropertyAttribute"/>.
        /// </summary>
        public bool IsConventionMapped { get; }

        /// <summary>
        /// Gets the mapped columns in declaration order. Never null.
        /// </summary>
        public IReadOnlyList<ColumnMetadata> Columns { get; }

        /// <summary>
        /// Gets the primary key columns in key order. Empty when the type has no key (for example, a projection type).
        /// </summary>
        public IReadOnlyList<ColumnMetadata> KeyColumns { get; }

        /// <summary>
        /// Gets whether the primary key spans more than one column.
        /// </summary>
        public bool HasCompositeKey => KeyColumns.Count > 1;

        /// <summary>
        /// Gets the database-generated column, or null. At most one column may be auto-increment.
        /// </summary>
        public ColumnMetadata? AutoIncrementColumn { get; }

        /// <summary>
        /// Gets the optimistic concurrency version column, or null.
        /// </summary>
        public ColumnMetadata? VersionColumn { get; }

        /// <summary>
        /// Gets version column behavior, or null when there is no version column.
        /// </summary>
        public VersionColumnInfo? VersionInfo { get; }

        /// <summary>
        /// Gets the soft-delete marker column, or null.
        /// </summary>
        public ColumnMetadata? SoftDeleteColumn { get; }

        /// <summary>
        /// Gets the navigation properties. Never null.
        /// </summary>
        public IReadOnlyList<NavigationMetadata> Navigations { get; }

        /// <summary>
        /// Gets the class-level composite index declarations. Never null.
        /// </summary>
        public IReadOnlyList<CompositeIndexAttribute> CompositeIndexes { get; }

        #endregion

        #region Private-Members

        private static readonly ConcurrentDictionary<Type, Lazy<EntityMetadata>> _Cache = new ConcurrentDictionary<Type, Lazy<EntityMetadata>>();

        private readonly Dictionary<string, ColumnMetadata> _ByColumnName;
        private readonly Dictionary<string, ColumnMetadata> _ByPropertyName;
        private readonly Dictionary<string, ColumnMetadata> _ByNormalizedName;
        private readonly Dictionary<string, NavigationMetadata> _NavigationsByName;
        private readonly Func<object> _Constructor;

        #endregion

        #region Constructors-and-Factories

        private EntityMetadata(Type type)
        {
            EntityType = type;
            _Constructor = MemberAccessorFactory.CreateConstructor(type);

            EntityAttribute? entityAttribute = type.GetCustomAttribute<EntityAttribute>();
            HasEntityAttribute = entityAttribute != null;
            TableName = entityAttribute?.Name ?? DurableMapping.ApplyNamingConvention(type.Name);

            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .OrderBy(p => p.MetadataToken)
                .ToArray();

            IsConventionMapped = !properties.Any(p => p.GetCustomAttribute<PropertyAttribute>() != null);

            NullabilityInfoContext nullability = new NullabilityInfoContext();
            List<ColumnMetadata> columns = new List<ColumnMetadata>();
            List<NavigationMetadata> navigations = new List<NavigationMetadata>();

            foreach (PropertyInfo property in properties)
            {
                NavigationMetadata? navigation = BuildNavigation(type, property);
                if (navigation != null)
                {
                    navigations.Add(navigation);
                    continue;
                }

                ColumnMetadata? column = BuildColumn(type, property, nullability, IsConventionMapped);
                if (column != null) columns.Add(column);
            }

            Columns = columns;
            KeyColumns = columns
                .Where(c => c.IsPrimaryKey)
                .Select((c, i) => new { Column = c, Declared = i })
                .OrderBy(x => x.Column.Property.GetCustomAttribute<PropertyAttribute>()?.KeyOrder ?? 0)
                .ThenBy(x => x.Declared)
                .Select(x => x.Column)
                .ToList();
            for (int i = 0; i < KeyColumns.Count; i++) KeyColumns[i].KeyOrdinal = i;

            List<ColumnMetadata> autoIncrement = columns.Where(c => c.IsAutoIncrement).ToList();
            if (autoIncrement.Count > 1)
                throw new InvalidOperationException("Entity " + type.Name + " declares more than one AutoIncrement column.");
            AutoIncrementColumn = autoIncrement.FirstOrDefault();

            List<ColumnMetadata> versions = columns.Where(c => c.IsVersion).ToList();
            if (versions.Count > 1)
                throw new InvalidOperationException("Entity " + type.Name + " declares more than one VersionColumn.");
            VersionColumn = versions.FirstOrDefault();
            if (VersionColumn != null)
            {
                VersionColumnAttribute versionAttribute = VersionColumn.Property.GetCustomAttribute<VersionColumnAttribute>()!;
                VersionInfo = new VersionColumnInfo
                {
                    ColumnName = VersionColumn.Name,
                    Property = VersionColumn.Property,
                    PropertyType = VersionColumn.PropertyType,
                    Type = versionAttribute.Type
                };
            }

            List<ColumnMetadata> softDeletes = columns.Where(c => c.IsSoftDelete).ToList();
            if (softDeletes.Count > 1)
                throw new InvalidOperationException("Entity " + type.Name + " declares more than one SoftDelete column.");
            SoftDeleteColumn = softDeletes.FirstOrDefault();
            if (SoftDeleteColumn != null
                && SoftDeleteColumn.ClrType != typeof(bool)
                && !((SoftDeleteColumn.ClrType == typeof(DateTime) || SoftDeleteColumn.ClrType == typeof(DateTimeOffset)) && SoftDeleteColumn.IsNullable))
            {
                throw new InvalidOperationException("SoftDelete column " + type.Name + "." + SoftDeleteColumn.Property.Name + " must be bool, DateTime? or DateTimeOffset?.");
            }

            Navigations = navigations;
            CompositeIndexes = type.GetCustomAttributes<CompositeIndexAttribute>(true).ToList();

            _ByColumnName = new Dictionary<string, ColumnMetadata>(StringComparer.OrdinalIgnoreCase);
            _ByPropertyName = new Dictionary<string, ColumnMetadata>(StringComparer.OrdinalIgnoreCase);
            _ByNormalizedName = new Dictionary<string, ColumnMetadata>(StringComparer.OrdinalIgnoreCase);
            foreach (ColumnMetadata column in columns)
            {
                _ByColumnName.TryAdd(column.Name, column);
                _ByPropertyName.TryAdd(column.Property.Name, column);
                _ByNormalizedName.TryAdd(Normalize(column.Name), column);
                _ByNormalizedName.TryAdd(Normalize(column.Property.Name), column);
            }

            _NavigationsByName = new Dictionary<string, NavigationMetadata>(StringComparer.Ordinal);
            foreach (NavigationMetadata navigation in navigations) _NavigationsByName[navigation.Name] = navigation;
        }

        /// <summary>
        /// Gets the cached metadata for a type, building it on first use.
        /// </summary>
        /// <param name="type">Entity or projection type. Must not be null and must have a parameterless constructor.</param>
        /// <returns>The metadata.</returns>
        /// <exception cref="ArgumentNullException">Thrown when type is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the mapping is invalid.</exception>
        public static EntityMetadata For(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            Lazy<EntityMetadata> lazy = _Cache.GetOrAdd(type, t => new Lazy<EntityMetadata>(() => new EntityMetadata(t), System.Threading.LazyThreadSafetyMode.ExecutionAndPublication));
            return lazy.Value;
        }

        /// <summary>
        /// Gets the cached metadata for a type, building it on first use.
        /// </summary>
        /// <typeparam name="T">Entity or projection type.</typeparam>
        /// <returns>The metadata.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the mapping is invalid.</exception>
        public static EntityMetadata For<T>()
        {
            return For(typeof(T));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a new instance of the entity using a compiled constructor.
        /// </summary>
        /// <returns>A new instance.</returns>
        public object CreateInstance()
        {
            return _Constructor();
        }

        /// <summary>
        /// Throws when the entity has no primary key; repositories require one.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when no primary key is declared.</exception>
        public void RequireKey()
        {
            if (KeyColumns.Count == 0)
                throw new InvalidOperationException(
                    "Entity " + EntityType.Name + " has no primary key. Mark a property with Flags.PrimaryKey" +
                    (IsConventionMapped ? " or name it 'Id' / '" + EntityType.Name + "Id'." : "."));
        }

        /// <summary>
        /// Finds a column by its column name (case-insensitive).
        /// </summary>
        /// <param name="columnName">Column name. Must not be null.</param>
        /// <returns>The column, or null.</returns>
        public ColumnMetadata? FindColumnByName(string columnName)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            _ByColumnName.TryGetValue(columnName, out ColumnMetadata? column);
            return column;
        }

        /// <summary>
        /// Finds a column by its property name (case-insensitive).
        /// </summary>
        /// <param name="propertyName">Property name. Must not be null.</param>
        /// <returns>The column, or null.</returns>
        public ColumnMetadata? FindColumnByProperty(string propertyName)
        {
            ArgumentNullException.ThrowIfNull(propertyName);
            _ByPropertyName.TryGetValue(propertyName, out ColumnMetadata? column);
            return column;
        }

        /// <summary>
        /// Finds the column for a property.
        /// </summary>
        /// <param name="property">Property. Must not be null.</param>
        /// <returns>The column, or null when the property is not mapped.</returns>
        public ColumnMetadata? FindColumn(PropertyInfo property)
        {
            ArgumentNullException.ThrowIfNull(property);
            if (_ByPropertyName.TryGetValue(property.Name, out ColumnMetadata? column) && column.Property.Name == property.Name)
                return column;
            return null;
        }

        /// <summary>
        /// Resolves a result-set column label to a mapped column: exact column name, then property name,
        /// then a match ignoring case and underscores (so first_name matches FirstName).
        /// </summary>
        /// <param name="label">Result column label. Must not be null.</param>
        /// <returns>The column, or null.</returns>
        public ColumnMetadata? MatchResultColumn(string label)
        {
            ArgumentNullException.ThrowIfNull(label);
            if (_ByColumnName.TryGetValue(label, out ColumnMetadata? column)) return column;
            if (_ByPropertyName.TryGetValue(label, out column)) return column;
            if (_ByNormalizedName.TryGetValue(Normalize(label), out column)) return column;
            return null;
        }

        /// <summary>
        /// Finds a navigation by property name (case-sensitive).
        /// </summary>
        /// <param name="propertyName">Navigation property name. Must not be null.</param>
        /// <returns>The navigation, or null.</returns>
        public NavigationMetadata? FindNavigation(string propertyName)
        {
            ArgumentNullException.ThrowIfNull(propertyName);
            _NavigationsByName.TryGetValue(propertyName, out NavigationMetadata? navigation);
            return navigation;
        }

        /// <summary>
        /// Reads the key of an entity. Single-column keys return the value; composite keys return an object array in key order.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <returns>The key value; may be null when unset.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entity is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity has no key.</exception>
        public object? GetKey(object entity)
        {
            ArgumentNullException.ThrowIfNull(entity);
            RequireKey();
            if (KeyColumns.Count == 1) return KeyColumns[0].GetValue(entity);
            object?[] values = new object?[KeyColumns.Count];
            for (int i = 0; i < KeyColumns.Count; i++) values[i] = KeyColumns[i].GetValue(entity);
            return values;
        }

        /// <summary>
        /// Splits an id argument into one value per key column.
        /// A single-column key accepts any scalar; a composite key accepts an <see cref="object"/> array (or any non-string
        /// <see cref="IEnumerable"/>) with one value per key column in key order.
        /// </summary>
        /// <param name="id">Key value or values. Must not be null.</param>
        /// <returns>One value per key column.</returns>
        /// <exception cref="ArgumentNullException">Thrown when id is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the number of values does not match the key.</exception>
        public object?[] SplitKey(object id)
        {
            ArgumentNullException.ThrowIfNull(id);
            RequireKey();
            if (KeyColumns.Count == 1)
            {
                if (id is object?[] single && single.Length == 1) return single;
                return new object?[] { id };
            }

            if (id is string || id is not IEnumerable enumerable)
                throw new ArgumentException("Entity " + EntityType.Name + " has a composite key of " + KeyColumns.Count + " columns; pass an object[] with one value per key column.", nameof(id));

            List<object?> values = new List<object?>();
            foreach (object? value in enumerable) values.Add(value);
            if (values.Count != KeyColumns.Count)
                throw new ArgumentException("Entity " + EntityType.Name + " key requires " + KeyColumns.Count + " values but " + values.Count + " were supplied.", nameof(id));
            return values.ToArray();
        }

        /// <summary>
        /// Determines whether a CLR type is stored as a single scalar column (not JSON).
        /// </summary>
        /// <param name="type">Type to test. Must not be null.</param>
        /// <returns>True for primitives, string, decimal, date/time types, Guid, enums, byte arrays and their nullable forms.</returns>
        public static bool IsScalarType(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            Type t = Nullable.GetUnderlyingType(type) ?? type;
            return t.IsPrimitive
                || t.IsEnum
                || t == typeof(string)
                || t == typeof(decimal)
                || t == typeof(DateTime)
                || t == typeof(DateTimeOffset)
                || t == typeof(TimeSpan)
                || t == typeof(DateOnly)
                || t == typeof(TimeOnly)
                || t == typeof(Guid)
                || t == typeof(byte[]);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return EntityType.Name + " -> " + TableName;
        }

        #endregion

        #region Private-Methods

        private static NavigationMetadata? BuildNavigation(Type owner, PropertyInfo property)
        {
            NavigationPropertyAttribute? reference = property.GetCustomAttribute<NavigationPropertyAttribute>();
            if (reference != null)
                return new NavigationMetadata(property, NavigationKind.Reference, owner, property.PropertyType, reference.ForeignKeyProperty, null, null);

            InverseNavigationPropertyAttribute? inverse = property.GetCustomAttribute<InverseNavigationPropertyAttribute>();
            if (inverse != null)
                return new NavigationMetadata(property, NavigationKind.Collection, owner, GetElementType(owner, property), inverse.InverseForeignKeyProperty, null, null);

            ManyToManyNavigationPropertyAttribute? manyToMany = property.GetCustomAttribute<ManyToManyNavigationPropertyAttribute>();
            if (manyToMany != null)
                return new NavigationMetadata(property, NavigationKind.ManyToMany, owner, GetElementType(owner, property), manyToMany.ThisEntityForeignKeyProperty, manyToMany.JunctionEntityType, manyToMany.RelatedEntityForeignKeyProperty);

            return null;
        }

        private static Type GetElementType(Type owner, PropertyInfo property)
        {
            Type type = property.PropertyType;
            if (type.IsArray) return type.GetElementType()!;
            if (type.IsGenericType)
            {
                Type[] args = type.GetGenericArguments();
                if (args.Length == 1) return args[0];
            }

            Type? enumerable = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (enumerable != null) return enumerable.GetGenericArguments()[0];
            throw new InvalidOperationException("Collection navigation " + owner.Name + "." + property.Name + " must be a generic collection type such as List<T>.");
        }

        private static ColumnMetadata? BuildColumn(Type owner, PropertyInfo property, NullabilityInfoContext nullability, bool conventionMapped)
        {
            PropertyAttribute? attribute = property.GetCustomAttribute<PropertyAttribute>();
            ValueConverterAttribute? converterAttribute = property.GetCustomAttribute<ValueConverterAttribute>();

            string name;
            Flags flags;
            int maxLength;

            if (attribute != null)
            {
                name = attribute.Name;
                flags = attribute.PropertyFlags;
                maxLength = attribute.MaxLength;
            }
            else
            {
                if (!conventionMapped) return null;
                if (property.GetCustomAttribute<NotMappedAttribute>() != null) return null;
                if (!property.CanRead || !property.CanWrite) return null;
                if (converterAttribute == null && !IsScalarType(property.PropertyType)) return null;

                name = DurableMapping.ApplyNamingConvention(property.Name);
                flags = Flags.None;
                maxLength = 0;
                if (IsConventionKey(owner, property))
                {
                    flags |= Flags.PrimaryKey;
                    Type keyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                    if (DurableMapping.ConventionKeysAreAutoIncrement && (keyType == typeof(int) || keyType == typeof(long)))
                        flags |= Flags.AutoIncrement;
                }
            }

            IValueConverter? converter = null;
            if (converterAttribute != null)
            {
                object? instance = MemberAccessorFactory.CreateConstructor(converterAttribute.ConverterType)();
                converter = (IValueConverter)instance;
            }

            bool isNullable;
            if (property.PropertyType.IsValueType)
            {
                isNullable = Nullable.GetUnderlyingType(property.PropertyType) != null;
            }
            else
            {
                NullabilityInfo info = nullability.Create(property);
                isNullable = info.WriteState != NullabilityState.NotNull;
            }

            bool isJson = converter == null && ((flags & Flags.Json) == Flags.Json || !IsScalarType(property.PropertyType));
            bool isPrimaryKey = (flags & Flags.PrimaryKey) == Flags.PrimaryKey;
            bool isAutoIncrement = (flags & Flags.AutoIncrement) == Flags.AutoIncrement;
            if (isPrimaryKey) isNullable = false;

            return new ColumnMetadata(
                name,
                property,
                flags,
                maxLength,
                isPrimaryKey,
                isAutoIncrement,
                isNullable,
                isJson,
                property.GetCustomAttribute<VersionColumnAttribute>() != null,
                property.GetCustomAttribute<SoftDeleteAttribute>() != null,
                converter,
                property.GetCustomAttribute<ForeignKeyAttribute>(),
                BuildDefaultValue(property),
                property.GetCustomAttributes<IndexAttribute>(true).ToList());
        }

        private static bool IsConventionKey(Type owner, PropertyInfo property)
        {
            foreach (string candidate in DurableMapping.KeyPropertyNames)
            {
                string resolved = candidate.Replace("{Type}", owner.Name, StringComparison.Ordinal);
                if (string.Equals(resolved, property.Name, StringComparison.OrdinalIgnoreCase)) return true;
            }

            return false;
        }

        private static DefaultValueProviderInfo? BuildDefaultValue(PropertyInfo property)
        {
            DefaultValueAttribute? attribute = property.GetCustomAttribute<DefaultValueAttribute>();
            if (attribute == null) return null;

            IDefaultValueProvider? provider = null;
            switch (attribute.ValueType)
            {
                case DefaultValueType.CurrentDateTimeUtc:
                    provider = new DefaultValueProviders.CurrentDateTimeUtcProvider();
                    break;
                case DefaultValueType.CurrentDateTimeLocal:
                    provider = new DefaultValueProviders.DelegateValueProvider(() => DateTime.Now, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.NewGuid:
                    provider = new DefaultValueProviders.NewGuidProvider();
                    break;
                case DefaultValueType.SequentialGuid:
                    provider = new DefaultValueProviders.SequentialGuidProvider();
                    break;
                case DefaultValueType.EmptyGuid:
                    provider = new DefaultValueProviders.StaticValueProvider(Guid.Empty, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.EmptyString:
                    provider = new DefaultValueProviders.StaticValueProvider(string.Empty, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.Zero:
                    provider = new DefaultValueProviders.StaticValueProvider(0, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.CurrentDateUtc:
                    provider = new DefaultValueProviders.DelegateValueProvider(() => DateTime.UtcNow.Date, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.CurrentDateLocal:
                    provider = new DefaultValueProviders.DelegateValueProvider(() => DateTime.Now.Date, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.True:
                    provider = new DefaultValueProviders.StaticValueProvider(true, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.False:
                    provider = new DefaultValueProviders.StaticValueProvider(false, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.StaticValue:
                    if (attribute.StaticValue != null)
                        provider = new DefaultValueProviders.StaticValueProvider(attribute.StaticValue, attribute.OnlyIfNull);
                    break;
                case DefaultValueType.CustomProvider:
                    if (attribute.ProviderType != null)
                        provider = (IDefaultValueProvider)MemberAccessorFactory.CreateConstructor(attribute.ProviderType)();
                    break;
            }

            return provider == null ? null : new DefaultValueProviderInfo(attribute, provider);
        }

        private static string Normalize(string name)
        {
            return name.Replace("_", string.Empty, StringComparison.Ordinal);
        }

        #endregion
    }
}
