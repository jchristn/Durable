namespace Durable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Description of a navigation property, built once per entity type and cached.
    /// Related metadata is resolved lazily so mutually-referencing entities do not recurse during construction.
    /// Thread safety: safe to share across threads.
    /// </summary>
    public sealed class NavigationMetadata
    {
        #region Public-Members

        /// <summary>
        /// Gets the navigation property. Never null.
        /// </summary>
        public PropertyInfo Property { get; }

        /// <summary>
        /// Gets the property name.
        /// </summary>
        public string Name => Property.Name;

        /// <summary>
        /// Gets the navigation kind.
        /// </summary>
        public NavigationKind Kind { get; }

        /// <summary>
        /// Gets the entity type that declares the navigation.
        /// </summary>
        public Type OwnerType { get; }

        /// <summary>
        /// Gets the related entity type (the element type for collections).
        /// </summary>
        public Type RelatedType { get; }

        /// <summary>
        /// Gets the junction entity type for many-to-many navigations; null otherwise.
        /// </summary>
        public Type? JunctionType { get; }

        /// <summary>
        /// Gets whether the navigation holds a collection.
        /// </summary>
        public bool IsCollection => Kind != NavigationKind.Reference;

        /// <summary>
        /// Gets the column on the owner entity used to match related rows:
        /// the foreign key for <see cref="NavigationKind.Reference"/>, otherwise the principal key.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the configured property names cannot be resolved.</exception>
        public ColumnMetadata LocalColumn => _Resolved.Value.LocalColumn;

        /// <summary>
        /// Gets the column on the related entity matched against <see cref="LocalColumn"/>
        /// (for many-to-many, the related key referenced by the junction).
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the configured property names cannot be resolved.</exception>
        public ColumnMetadata RemoteColumn => _Resolved.Value.RemoteColumn;

        /// <summary>
        /// Gets the junction column referencing the owner; null unless many-to-many.
        /// </summary>
        public ColumnMetadata? JunctionLocalColumn => _Resolved.Value.JunctionLocalColumn;

        /// <summary>
        /// Gets the junction column referencing the related entity; null unless many-to-many.
        /// </summary>
        public ColumnMetadata? JunctionRemoteColumn => _Resolved.Value.JunctionRemoteColumn;

        #endregion

        #region Private-Members

        private readonly string _ForeignKeyPropertyName;
        private readonly string? _JunctionRemotePropertyName;
        private readonly Action<object, object?> _Setter;
        private readonly Func<object, object?> _Getter;
        private readonly Func<IList>? _ListFactory;
        private readonly Lazy<ResolvedNavigation> _Resolved;

        #endregion

        #region Constructors-and-Factories

        internal NavigationMetadata(PropertyInfo property, NavigationKind kind, Type ownerType, Type relatedType, string foreignKeyPropertyName, Type? junctionType, string? junctionRemotePropertyName)
        {
            Property = property;
            Kind = kind;
            OwnerType = ownerType;
            RelatedType = relatedType;
            JunctionType = junctionType;
            _ForeignKeyPropertyName = foreignKeyPropertyName;
            _JunctionRemotePropertyName = junctionRemotePropertyName;
            _Setter = MemberAccessorFactory.CreateSetter(property);
            _Getter = MemberAccessorFactory.CreateGetter(property);
            _ListFactory = kind == NavigationKind.Reference ? null : MemberAccessorFactory.CreateListFactory(relatedType);
            _Resolved = new Lazy<ResolvedNavigation>(Resolve, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Assigns the navigation value on an owner entity.
        /// </summary>
        /// <param name="owner">Owner entity. Must not be null.</param>
        /// <param name="value">Related entity, or a list for collection navigations; may be null.</param>
        public void SetValue(object owner, object? value)
        {
            _Setter(owner, value);
        }

        /// <summary>
        /// Reads the navigation value from an owner entity.
        /// </summary>
        /// <param name="owner">Owner entity. Must not be null.</param>
        /// <returns>The navigation value; may be null.</returns>
        public object? GetValue(object owner)
        {
            return _Getter(owner);
        }

        /// <summary>
        /// Creates an empty list suitable for assignment to a collection navigation.
        /// </summary>
        /// <returns>An empty list.</returns>
        /// <exception cref="InvalidOperationException">Thrown for reference navigations.</exception>
        public IList CreateList()
        {
            if (_ListFactory == null)
                throw new InvalidOperationException("Navigation '" + Name + "' is not a collection.");
            return _ListFactory();
        }

        #endregion

        #region Private-Methods

        private ResolvedNavigation Resolve()
        {
            EntityMetadata owner = EntityMetadata.For(OwnerType);
            EntityMetadata related = EntityMetadata.For(RelatedType);
            ResolvedNavigation result = new ResolvedNavigation();

            switch (Kind)
            {
                case NavigationKind.Reference:
                    {
                        ColumnMetadata fk = RequireColumn(owner, _ForeignKeyPropertyName);
                        result.LocalColumn = fk;
                        result.RemoteColumn = ResolveReferencedColumn(fk, related);
                        break;
                    }
                case NavigationKind.Collection:
                    {
                        ColumnMetadata fk = RequireColumn(related, _ForeignKeyPropertyName);
                        result.RemoteColumn = fk;
                        result.LocalColumn = ResolveReferencedColumn(fk, owner);
                        break;
                    }
                case NavigationKind.ManyToMany:
                    {
                        EntityMetadata junction = EntityMetadata.For(JunctionType!);
                        ColumnMetadata junctionLocal = RequireColumn(junction, _ForeignKeyPropertyName);
                        ColumnMetadata junctionRemote = RequireColumn(junction, _JunctionRemotePropertyName!);
                        result.JunctionLocalColumn = junctionLocal;
                        result.JunctionRemoteColumn = junctionRemote;
                        result.LocalColumn = ResolveReferencedColumn(junctionLocal, owner);
                        result.RemoteColumn = ResolveReferencedColumn(junctionRemote, related);
                        break;
                    }
            }

            return result;
        }

        private ColumnMetadata RequireColumn(EntityMetadata entity, string propertyName)
        {
            ColumnMetadata? column = entity.FindColumnByProperty(propertyName);
            if (column == null)
                throw new InvalidOperationException(
                    "Navigation '" + OwnerType.Name + "." + Name + "' refers to property '" + propertyName +
                    "' which is not a mapped column on " + entity.EntityType.Name + ".");
            return column;
        }

        private static ColumnMetadata ResolveReferencedColumn(ColumnMetadata foreignKey, EntityMetadata principal)
        {
            if (foreignKey.ForeignKey != null && principal.EntityType == foreignKey.ForeignKey.ReferencedType)
            {
                ColumnMetadata? referenced = principal.FindColumnByProperty(foreignKey.ForeignKey.ReferencedProperty);
                if (referenced != null) return referenced;
            }

            if (principal.KeyColumns.Count != 1)
                throw new InvalidOperationException("Navigation targets entity " + principal.EntityType.Name + " which has a composite key; declare [ForeignKey] with an explicit referenced property.");
            return principal.KeyColumns[0];
        }

        #endregion
    }
}
