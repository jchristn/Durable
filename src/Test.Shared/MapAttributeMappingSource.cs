namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Durable;

    /// <summary>
    /// A test <see cref="IEntityMappingSource"/> that translates the test-only Map* attributes (<see cref="MapTableAttribute"/>,
    /// <see cref="MapColumnAttribute"/> and friends) into Durable's attributes, the way an adapter for another attribute system
    /// would. With the parameterless constructor it describes every class marked <see cref="MapTableAttribute"/>; the other
    /// constructor limits it to the listed types, so it can be set as <see cref="DurableMapping.MappingSource"/> without claiming
    /// types other suites own.
    /// Thread safety: immutable; safe to call concurrently.
    /// </summary>
    public sealed class MapAttributeMappingSource : IEntityMappingSource
    {
        #region Private-Members

        private readonly HashSet<Type>? _Only;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a source describing every class marked <see cref="MapTableAttribute"/>.
        /// </summary>
        public MapAttributeMappingSource()
        {
        }

        /// <summary>
        /// Instantiates a source describing only the listed types.
        /// </summary>
        /// <param name="only">Types to describe. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when only is null.</exception>
        public MapAttributeMappingSource(params Type[] only)
        {
            ArgumentNullException.ThrowIfNull(only);
            _Only = new HashSet<Type>(only);
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public bool Describes(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            if (_Only != null) return _Only.Contains(entityType);
            return entityType.GetCustomAttribute<MapTableAttribute>() != null;
        }

        /// <inheritdoc />
        public EntityAttribute? GetEntityAttribute(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            MapTableAttribute? table = entityType.GetCustomAttribute<MapTableAttribute>();
            return table != null ? new EntityAttribute(table.Name) : null;
        }

        /// <inheritdoc />
        public IEnumerable<Attribute> GetPropertyAttributes(Type entityType, PropertyInfo property)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(property);
            List<Attribute> result = new List<Attribute>();

            MapColumnAttribute? column = property.GetCustomAttribute<MapColumnAttribute>();
            if (column != null)
            {
                Flags flags = Flags.None;
                if (column.Key) flags |= Flags.PrimaryKey;
                if (column.Identity) flags |= Flags.AutoIncrement;
                if (column.Length > 0) flags |= Flags.String;
                result.Add(new PropertyAttribute(column.Name, flags, column.Length) { KeyOrder = column.KeyOrder });
            }

            if (property.GetCustomAttribute<MapIgnoreAttribute>() != null) result.Add(new NotMappedAttribute());
            if (property.GetCustomAttribute<MapVersionAttribute>() != null) result.Add(new VersionColumnAttribute());
            if (property.GetCustomAttribute<MapSoftDeleteAttribute>() != null) result.Add(new SoftDeleteAttribute());
            if (property.GetCustomAttribute<MapNewGuidAttribute>() != null) result.Add(new DefaultValueAttribute(DefaultValueType.NewGuid));

            MapReferenceAttribute? reference = property.GetCustomAttribute<MapReferenceAttribute>();
            if (reference != null) result.Add(new NavigationPropertyAttribute(reference.ForeignKeyProperty));

            MapCollectionAttribute? collection = property.GetCustomAttribute<MapCollectionAttribute>();
            if (collection != null) result.Add(new InverseNavigationPropertyAttribute(collection.InverseForeignKeyProperty));

            MapManyToManyAttribute? manyToMany = property.GetCustomAttribute<MapManyToManyAttribute>();
            if (manyToMany != null) result.Add(new ManyToManyNavigationPropertyAttribute(manyToMany.Junction, manyToMany.ThisKey, manyToMany.OtherKey));

            MapForeignKeyAttribute? foreignKey = property.GetCustomAttribute<MapForeignKeyAttribute>();
            if (foreignKey != null) result.Add(new ForeignKeyAttribute(foreignKey.Referenced, foreignKey.Property));

            MapConverterAttribute? converter = property.GetCustomAttribute<MapConverterAttribute>();
            if (converter != null) result.Add(new ValueConverterAttribute(converter.Converter));

            foreach (MapIndexAttribute index in property.GetCustomAttributes<MapIndexAttribute>())
                result.Add(new IndexAttribute(index.Name, index.Unique));

            return result;
        }

        /// <inheritdoc />
        public IEnumerable<CompositeIndexAttribute> GetCompositeIndexes(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return entityType.GetCustomAttributes<MapCompositeIndexAttribute>()
                .Select(i => new CompositeIndexAttribute(i.Name, i.Columns) { IsUnique = i.Unique })
                .ToList();
        }

        #endregion
    }
}
