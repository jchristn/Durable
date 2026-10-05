namespace Durable.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Loads Include/ThenInclude navigations through an <see cref="IRepositoryBackend"/> with split queries, the
    /// backend-neutral counterpart of the SQL engine's include loader. For each navigation the distinct owner keys are
    /// collected (normalized with <see cref="KeyNormalizer"/>), related rows are queried with an <see cref="InNode"/>
    /// filter in chunks of <see cref="ChunkSize"/> keys (excluding soft-deleted related rows, ordered by the related key),
    /// assigned to the owners, and nested includes are loaded for the related rows. Many-to-many navigations query the
    /// junction entity by owner key and then the related entity by the junction's remote keys, so no joins are needed.
    /// Reference navigations with no match are set to null; collection navigations always receive a list (empty when
    /// nothing matches).
    /// Thread safety: safe for concurrent use once configured; do not change <see cref="ChunkSize"/> while loading.
    /// </summary>
    public sealed class BackendIncludeLoader
    {
        #region Public-Members

        /// <summary>
        /// Gets the backend related rows are read from. Never null.
        /// </summary>
        public IRepositoryBackend Backend { get; }

        /// <summary>
        /// Gets or sets the maximum number of keys in one related-row query. Default: 500. Minimum: 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
        public int ChunkSize
        {
            get => _ChunkSize;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "ChunkSize must be at least 1.");
                _ChunkSize = value;
            }
        }

        #endregion

        #region Private-Members

        private int _ChunkSize = 500;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a loader.
        /// </summary>
        /// <param name="backend">Backend. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when backend is null.</exception>
        public BackendIncludeLoader(IRepositoryBackend backend)
        {
            Backend = backend ?? throw new ArgumentNullException(nameof(backend));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Loads navigations for owner entities.
        /// </summary>
        /// <param name="parents">Owner entities. Must not be null.</param>
        /// <param name="nodes">Include nodes rooted at the owners' type. Must not be null.</param>
        /// <param name="transaction">Transaction; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when parents or nodes is null.</exception>
        public void Load(IReadOnlyList<object> parents, IReadOnlyList<IncludeNode> nodes, ITransaction? transaction)
        {
            SyncBridge.Run(() => LoadAsync(parents, nodes, transaction, CancellationToken.None));
        }

        /// <summary>
        /// Loads navigations for owner entities.
        /// </summary>
        /// <param name="parents">Owner entities. Must not be null.</param>
        /// <param name="nodes">Include nodes rooted at the owners' type. Must not be null.</param>
        /// <param name="transaction">Transaction; null for none.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when parents or nodes is null.</exception>
        public async Task LoadAsync(IReadOnlyList<object> parents, IReadOnlyList<IncludeNode> nodes, ITransaction? transaction, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(parents);
            ArgumentNullException.ThrowIfNull(nodes);
            if (parents.Count == 0) return;

            foreach (IncludeNode node in nodes)
            {
                token.ThrowIfCancellationRequested();
                List<object> related = node.Navigation.Kind == NavigationKind.ManyToMany
                    ? await LoadManyToManyAsync(node.Navigation, parents, transaction, token).ConfigureAwait(false)
                    : await LoadDirectAsync(node.Navigation, parents, transaction, token).ConfigureAwait(false);
                if (node.Children.Count > 0) await LoadAsync(related, node.Children, transaction, token).ConfigureAwait(false);
            }
        }

        #endregion

        #region Private-Methods

        private async Task<List<object>> LoadDirectAsync(NavigationMetadata navigation, IReadOnlyList<object> parents, ITransaction? transaction, CancellationToken token)
        {
            List<object> keys = DistinctKeys(parents, navigation.LocalColumn);
            EntityMetadata related = EntityMetadata.For(navigation.RelatedType);
            List<object> rows = keys.Count == 0
                ? new List<object>()
                : await QueryByKeysAsync(related, navigation.RemoteColumn, keys, true, transaction, token).ConfigureAwait(false);

            List<object> loaded = new List<object>(rows.Count);
            if (navigation.Kind == NavigationKind.Reference)
            {
                Dictionary<object, object> byKey = new Dictionary<object, object>();
                foreach (object row in rows)
                {
                    object? key = KeyNormalizer.Normalize(navigation.RemoteColumn.GetValue(row));
                    if (key != null && !byKey.ContainsKey(key))
                    {
                        byKey[key] = row;
                        loaded.Add(row);
                    }
                }

                foreach (object parent in parents)
                {
                    object? key = KeyNormalizer.Normalize(navigation.LocalColumn.GetValue(parent));
                    navigation.SetValue(parent, key != null && byKey.TryGetValue(key, out object? match) ? match : null);
                }

                return loaded;
            }

            Dictionary<object, List<object>> groups = new Dictionary<object, List<object>>();
            foreach (object row in rows)
            {
                object? key = KeyNormalizer.Normalize(navigation.RemoteColumn.GetValue(row));
                if (key == null) continue;
                if (!groups.TryGetValue(key, out List<object>? group))
                {
                    group = new List<object>();
                    groups[key] = group;
                }

                group.Add(row);
                loaded.Add(row);
            }

            AssignLists(navigation, parents, groups);
            return loaded;
        }

        private async Task<List<object>> LoadManyToManyAsync(NavigationMetadata navigation, IReadOnlyList<object> parents, ITransaction? transaction, CancellationToken token)
        {
            ColumnMetadata junctionLocal = navigation.JunctionLocalColumn!;
            ColumnMetadata junctionRemote = navigation.JunctionRemoteColumn!;
            EntityMetadata junction = EntityMetadata.For(navigation.JunctionType!);
            EntityMetadata related = EntityMetadata.For(navigation.RelatedType);

            List<object> ownerKeys = DistinctKeys(parents, navigation.LocalColumn);
            List<object> links = ownerKeys.Count == 0
                ? new List<object>()
                : await QueryByKeysAsync(junction, junctionLocal, ownerKeys, false, transaction, token).ConfigureAwait(false);

            Dictionary<object, List<object>> ownersByRemote = new Dictionary<object, List<object>>();
            Dictionary<object, object> remoteKeys = new Dictionary<object, object>();
            foreach (object link in links)
            {
                object? owner = KeyNormalizer.Normalize(junctionLocal.GetValue(link));
                object? remoteValue = junctionRemote.GetValue(link);
                object? remote = KeyNormalizer.Normalize(remoteValue);
                if (owner == null || remote == null) continue;
                if (!ownersByRemote.TryGetValue(remote, out List<object>? owners))
                {
                    owners = new List<object>();
                    ownersByRemote[remote] = owners;
                    remoteKeys[remote] = remoteValue!;
                }

                owners.Add(owner);
            }

            List<object> rows = remoteKeys.Count == 0
                ? new List<object>()
                : await QueryByKeysAsync(related, navigation.RemoteColumn, new List<object>(remoteKeys.Values), true, transaction, token).ConfigureAwait(false);

            Dictionary<object, List<object>> groups = new Dictionary<object, List<object>>();
            List<object> loaded = new List<object>(rows.Count);
            foreach (object row in rows)
            {
                object? remote = KeyNormalizer.Normalize(navigation.RemoteColumn.GetValue(row));
                if (remote == null || !ownersByRemote.TryGetValue(remote, out List<object>? owners)) continue;
                loaded.Add(row);
                foreach (object owner in owners)
                {
                    if (!groups.TryGetValue(owner, out List<object>? group))
                    {
                        group = new List<object>();
                        groups[owner] = group;
                    }

                    group.Add(row);
                }
            }

            AssignLists(navigation, parents, groups);
            return loaded;
        }

        private static void AssignLists(NavigationMetadata navigation, IReadOnlyList<object> parents, Dictionary<object, List<object>> groups)
        {
            foreach (object parent in parents)
            {
                IList list = navigation.CreateList();
                object? key = KeyNormalizer.Normalize(navigation.LocalColumn.GetValue(parent));
                if (key != null && groups.TryGetValue(key, out List<object>? children))
                {
                    foreach (object child in children) list.Add(child);
                }

                navigation.SetValue(parent, list);
            }
        }

        private static List<object> DistinctKeys(IReadOnlyList<object> parents, ColumnMetadata column)
        {
            Dictionary<object, object> keys = new Dictionary<object, object>();
            foreach (object parent in parents)
            {
                object? value = column.GetValue(parent);
                object? normalized = KeyNormalizer.Normalize(value);
                if (normalized != null && !keys.ContainsKey(normalized)) keys[normalized] = value!;
            }

            return new List<object>(keys.Values);
        }

        private async Task<List<object>> QueryByKeysAsync(EntityMetadata metadata, ColumnMetadata keyColumn, List<object> keys, bool excludeSoftDeleted, ITransaction? transaction, CancellationToken token)
        {
            List<object> rows = new List<object>();
            int chunkSize = _ChunkSize;
            for (int offset = 0; offset < keys.Count; offset += chunkSize)
            {
                token.ThrowIfCancellationRequested();
                List<object?> chunk = new List<object?>(keys.GetRange(offset, Math.Min(chunkSize, keys.Count - offset)));
                QuerySource source = new QuerySource(metadata, "include");
                QueryModel model = new QueryModel(source) { Transaction = transaction };
                InNode membership = new InNode(new ColumnNode(source, keyColumn), chunk, false, StringMatchMode.Database);
                model.Filter = QueryConditions.And(new[] { membership, excludeSoftDeleted ? QueryConditions.NotSoftDeleted(source) : null });
                foreach (ColumnMetadata key in metadata.KeyColumns) model.Orderings.Add(QueryConditions.OrderBy(source, key, false));
                await foreach (object row in Backend.QueryAsync(model, token).ConfigureAwait(false)) rows.Add(row);
            }

            return rows;
        }

        #endregion
    }
}
