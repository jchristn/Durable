namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;

    /// <summary>
    /// Discovers the foreign keys a <see cref="LiteGraphBackend"/> maintains as edges. Registering an entity type walks
    /// every type reachable from it through foreign keys and navigations (both directions), so an inverse or many-to-many
    /// navigation declared on the principal is known as soon as any related type is used. A foreign key qualifies when it
    /// references the principal's single-column primary key.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    internal sealed class LiteGraphRelationshipRegistry
    {
        #region Private-Members

        private readonly object _Lock = new object();
        private readonly ConcurrentDictionary<Type, byte> _Visited = new ConcurrentDictionary<Type, byte>();
        private readonly Dictionary<string, LiteGraphRelationship> _ById = new Dictionary<string, LiteGraphRelationship>(StringComparer.Ordinal);
        private volatile Dictionary<Type, LiteGraphRelationship[]> _ByDependent = new Dictionary<Type, LiteGraphRelationship[]>();
        private volatile Dictionary<Type, LiteGraphRelationship[]> _ByPrincipal = new Dictionary<Type, LiteGraphRelationship[]>();

        #endregion

        #region Public-Methods

        /// <summary>
        /// Registers an entity type and every type reachable from it.
        /// </summary>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        public void Register(EntityMetadata metadata)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            if (_Visited.ContainsKey(metadata.EntityType)) return;

            lock (_Lock)
            {
                if (_Visited.ContainsKey(metadata.EntityType)) return;
                Queue<EntityMetadata> pending = new Queue<EntityMetadata>();
                HashSet<Type> seen = new HashSet<Type>();
                pending.Enqueue(metadata);
                bool changed = false;
                while (pending.Count > 0)
                {
                    EntityMetadata current = pending.Dequeue();
                    if (_Visited.ContainsKey(current.EntityType) || !seen.Add(current.EntityType)) continue;
                    foreach (EntityMetadata related in Discover(current, ref changed))
                    {
                        if (!_Visited.ContainsKey(related.EntityType) && !seen.Contains(related.EntityType)) pending.Enqueue(related);
                    }
                }

                if (changed) Rebuild();
                foreach (Type type in seen) _Visited.TryAdd(type, 0);
            }
        }

        /// <summary>
        /// Returns the relationships in which an entity is the dependent (its foreign keys).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The relationships. Never null.</returns>
        public IReadOnlyList<LiteGraphRelationship> ForDependent(Type entityType)
        {
            return _ByDependent.TryGetValue(entityType, out LiteGraphRelationship[]? found) ? found : Array.Empty<LiteGraphRelationship>();
        }

        /// <summary>
        /// Returns the relationships in which an entity is the principal (foreign keys referencing it).
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <returns>The relationships. Never null.</returns>
        public IReadOnlyList<LiteGraphRelationship> ForPrincipal(Type entityType)
        {
            return _ByPrincipal.TryGetValue(entityType, out LiteGraphRelationship[]? found) ? found : Array.Empty<LiteGraphRelationship>();
        }

        /// <summary>
        /// Returns every registered relationship.
        /// </summary>
        /// <returns>The relationships ordered by identifier. Never null.</returns>
        public IReadOnlyList<LiteGraphRelationship> All()
        {
            lock (_Lock)
            {
                return _ById.Values.OrderBy(r => r.Id, StringComparer.Ordinal).ToList();
            }
        }

        #endregion

        #region Private-Methods

        private List<EntityMetadata> Discover(EntityMetadata current, ref bool changed)
        {
            List<EntityMetadata> related = new List<EntityMetadata>();

            foreach (ColumnMetadata column in current.Columns)
            {
                if (column.ForeignKey == null) continue;
                EntityMetadata? principal = TryMetadata(column.ForeignKey.ReferencedType);
                if (principal == null) continue;
                related.Add(principal);
                if (IsSingleKey(principal, column.ForeignKey.ReferencedProperty)) changed |= Add(current, column, principal);
            }

            foreach (NavigationMetadata navigation in current.Navigations)
            {
                try
                {
                    switch (navigation.Kind)
                    {
                        case NavigationKind.Reference:
                            {
                                EntityMetadata? principal = TryMetadata(navigation.RelatedType);
                                if (principal == null) break;
                                related.Add(principal);
                                if (IsSingleKey(principal, navigation.RemoteColumn)) changed |= Add(current, navigation.LocalColumn, principal);
                                break;
                            }
                        case NavigationKind.Collection:
                            {
                                EntityMetadata? dependent = TryMetadata(navigation.RelatedType);
                                if (dependent == null) break;
                                related.Add(dependent);
                                if (IsSingleKey(current, navigation.LocalColumn)) changed |= Add(dependent, navigation.RemoteColumn, current);
                                break;
                            }
                        case NavigationKind.ManyToMany:
                            {
                                EntityMetadata? junction = navigation.JunctionType == null ? null : TryMetadata(navigation.JunctionType);
                                EntityMetadata? other = TryMetadata(navigation.RelatedType);
                                if (junction == null || other == null) break;
                                related.Add(junction);
                                related.Add(other);
                                if (navigation.JunctionLocalColumn != null && IsSingleKey(current, navigation.LocalColumn))
                                    changed |= Add(junction, navigation.JunctionLocalColumn, current);
                                if (navigation.JunctionRemoteColumn != null && IsSingleKey(other, navigation.RemoteColumn))
                                    changed |= Add(junction, navigation.JunctionRemoteColumn, other);
                                break;
                            }
                    }
                }
                catch (InvalidOperationException)
                {
                    // A navigation whose property names cannot be resolved has no edge; Durable reports it when it is used.
                }
            }

            return related;
        }

        private bool Add(EntityMetadata dependent, ColumnMetadata foreignKey, EntityMetadata principal)
        {
            string label = foreignKey.Property.Name;
            foreach (NavigationMetadata navigation in dependent.Navigations)
            {
                if (navigation.Kind != NavigationKind.Reference || navigation.RelatedType != principal.EntityType) continue;
                try
                {
                    if (ReferenceEquals(navigation.LocalColumn, foreignKey))
                    {
                        label = navigation.Name;
                        break;
                    }
                }
                catch (InvalidOperationException)
                {
                    // Unresolvable navigation: keep the foreign key property name as the label.
                }
            }

            LiteGraphRelationship relationship = new LiteGraphRelationship(dependent, foreignKey, principal, label);
            if (_ById.ContainsKey(relationship.Id)) return false;
            _ById[relationship.Id] = relationship;
            return true;
        }

        private void Rebuild()
        {
            _ByDependent = _ById.Values.GroupBy(r => r.Dependent.EntityType).ToDictionary(g => g.Key, g => g.OrderBy(r => r.Id, StringComparer.Ordinal).ToArray());
            _ByPrincipal = _ById.Values.GroupBy(r => r.Principal.EntityType).ToDictionary(g => g.Key, g => g.OrderBy(r => r.Id, StringComparer.Ordinal).ToArray());
        }

        private static bool IsSingleKey(EntityMetadata principal, string propertyName)
        {
            return principal.KeyColumns.Count == 1 && string.Equals(principal.KeyColumns[0].Property.Name, propertyName, StringComparison.Ordinal);
        }

        private static bool IsSingleKey(EntityMetadata principal, ColumnMetadata column)
        {
            return principal.KeyColumns.Count == 1 && ReferenceEquals(principal.KeyColumns[0], column);
        }

        private static EntityMetadata? TryMetadata(Type type)
        {
            try
            {
                EntityMetadata metadata = EntityMetadata.For(type);
                return metadata.KeyColumns.Count > 0 ? metadata : null;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        #endregion
    }
}
