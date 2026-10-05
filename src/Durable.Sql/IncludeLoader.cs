namespace Durable.Sql
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Loads navigation properties for already-materialized entities with one query per navigation (split per
    /// parameter-limit chunk of keys): <c>SELECT related.* FROM related WHERE key IN (...)</c>, joined through the junction
    /// table for many-to-many. Root paging is unaffected and there is no cartesian explosion. Soft-deleted related rows are excluded.
    /// Thread safety: not thread-safe; create per query execution.
    /// </summary>
    public sealed class IncludeLoader
    {
        #region Private-Members

        private const string OwnerColumnAlias = "durable_owner_key";
        private readonly SqlCommandExecutor _Executor;
        private readonly IDataTypeConverter _Converter;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a loader.
        /// </summary>
        /// <param name="executor">Executor. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public IncludeLoader(SqlCommandExecutor executor, IDataTypeConverter converter)
        {
            _Executor = executor ?? throw new ArgumentNullException(nameof(executor));
            _Converter = converter ?? throw new ArgumentNullException(nameof(converter));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Loads the include tree for the given parents.
        /// </summary>
        /// <param name="lease">Connection lease with no open reader. Must not be null.</param>
        /// <param name="parents">Parent entities. Must not be null.</param>
        /// <param name="nodes">Include nodes. Must not be null.</param>
        public void Load(ConnectionLease lease, IReadOnlyList<object> parents, IReadOnlyList<IncludeNode> nodes)
        {
            ArgumentNullException.ThrowIfNull(lease);
            ArgumentNullException.ThrowIfNull(parents);
            ArgumentNullException.ThrowIfNull(nodes);
            if (parents.Count == 0) return;

            foreach (IncludeNode node in nodes)
            {
                List<IncludedRow> rows = new List<IncludedRow>();
                foreach (SqlStatement statement in BuildStatements(node.Navigation, parents))
                {
                    Func<DbDataReader, IncludedRow> map = CreateMapper(node.Navigation);
                    foreach (IncludedRow row in _Executor.Query(lease, statement, "INCLUDE", map)) rows.Add(row);
                }

                List<object> related = Assign(node.Navigation, parents, rows);
                if (node.Children.Count > 0) Load(lease, related, node.Children);
            }
        }

        /// <summary>
        /// Loads the include tree for the given parents.
        /// </summary>
        /// <param name="lease">Connection lease with no open reader. Must not be null.</param>
        /// <param name="parents">Parent entities. Must not be null.</param>
        /// <param name="nodes">Include nodes. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        public async Task LoadAsync(ConnectionLease lease, IReadOnlyList<object> parents, IReadOnlyList<IncludeNode> nodes, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(lease);
            ArgumentNullException.ThrowIfNull(parents);
            ArgumentNullException.ThrowIfNull(nodes);
            if (parents.Count == 0) return;

            foreach (IncludeNode node in nodes)
            {
                token.ThrowIfCancellationRequested();
                List<IncludedRow> rows = new List<IncludedRow>();
                foreach (SqlStatement statement in BuildStatements(node.Navigation, parents))
                {
                    Func<DbDataReader, IncludedRow> map = CreateMapper(node.Navigation);
                    await foreach (IncludedRow row in _Executor.QueryAsync(lease, statement, "INCLUDE", map, token).ConfigureAwait(false)) rows.Add(row);
                }

                List<object> related = Assign(node.Navigation, parents, rows);
                if (node.Children.Count > 0) await LoadAsync(lease, related, node.Children, token).ConfigureAwait(false);
            }
        }

        #endregion

        #region Private-Methods

        private IEnumerable<SqlStatement> BuildStatements(NavigationMetadata navigation, IReadOnlyList<object> parents)
        {
            ColumnMetadata local = navigation.LocalColumn;
            Dictionary<object, object> keys = new Dictionary<object, object>();
            foreach (object parent in parents)
            {
                object? value = local.GetValue(parent);
                object? normalized = KeyNormalizer.Normalize(value);
                if (normalized != null && !keys.ContainsKey(normalized)) keys[normalized] = value!;
            }

            if (keys.Count == 0) yield break;

            ISqlDialect dialect = _Executor.Dialect;
            int chunkSize = Math.Max(1, dialect.MaxParameters - 50);
            List<object> values = keys.Values.ToList();
            EntityMetadata related = EntityMetadata.For(navigation.RelatedType);

            for (int offset = 0; offset < values.Count; offset += chunkSize)
            {
                List<object> chunk = values.GetRange(offset, Math.Min(chunkSize, values.Count - offset));
                SqlStatementBuilder builder = new SqlStatementBuilder(dialect);
                SqlExpressionTranslator translator = new SqlExpressionTranslator(builder, _Converter);
                TableSource relatedSource = new TableSource("r", related);

                string keySql;
                ColumnMetadata keyColumn;
                builder.Append("SELECT r.*");
                if (navigation.Kind == NavigationKind.ManyToMany)
                {
                    EntityMetadata junction = EntityMetadata.For(navigation.JunctionType!);
                    TableSource junctionSource = new TableSource("j", junction);
                    keyColumn = navigation.JunctionLocalColumn!;
                    keySql = translator.ColumnSql(junctionSource, keyColumn);
                    builder.Append(", ").Append(keySql).Append(" AS ").AppendIdentifier(OwnerColumnAlias)
                        .Append(" FROM ").AppendIdentifier(related.TableName).Append(" r INNER JOIN ").AppendIdentifier(junction.TableName)
                        .Append(" j ON ").Append(translator.ColumnSql(junctionSource, navigation.JunctionRemoteColumn!))
                        .Append(" = ").Append(translator.ColumnSql(relatedSource, navigation.RemoteColumn));
                }
                else
                {
                    keyColumn = navigation.RemoteColumn;
                    keySql = translator.ColumnSql(relatedSource, keyColumn);
                    builder.Append(" FROM ").AppendIdentifier(related.TableName).Append(" r");
                }

                builder.Append(" WHERE ").Append(keySql).Append(" IN (");
                for (int i = 0; i < chunk.Count; i++)
                {
                    if (i > 0) builder.Append(", ");
                    builder.Append(translator.Parameter(chunk[i], keyColumn));
                }

                builder.Append(")");
                string? softDelete = translator.SoftDeleteFilter(relatedSource);
                if (softDelete != null) builder.Append(" AND ").Append(softDelete);
                if (related.KeyColumns.Count > 0)
                {
                    builder.Append(" ORDER BY ").Append(string.Join(", ", related.KeyColumns.Select(c => translator.ColumnSql(relatedSource, c))));
                }

                yield return builder.Build();
            }
        }

        private Func<DbDataReader, IncludedRow> CreateMapper(NavigationMetadata navigation)
        {
            EntityMetadata related = EntityMetadata.For(navigation.RelatedType);
            RowMaterializer? materializer = null;
            int ownerOrdinal = -1;
            IDataTypeConverter converter = _Converter;
            bool inline = RowReaderCompiler.CanInline(converter);

            return reader =>
            {
                if (materializer == null)
                {
                    materializer = RowMaterializer.For(related, reader);
                    if (navigation.Kind == NavigationKind.ManyToMany) ownerOrdinal = reader.GetOrdinal(OwnerColumnAlias);
                }

                object entity = materializer.Materialize(reader, converter, inline);
                object? ownerKey;
                if (navigation.Kind == NavigationKind.ManyToMany)
                {
                    ColumnMetadata junctionLocal = navigation.JunctionLocalColumn!;
                    ownerKey = reader.IsDBNull(ownerOrdinal) ? null : converter.ConvertFromDatabase(reader.GetValue(ownerOrdinal), junctionLocal.PropertyType, junctionLocal);
                }
                else
                {
                    ownerKey = navigation.RemoteColumn.GetValue(entity);
                }

                return new IncludedRow(entity, KeyNormalizer.Normalize(ownerKey));
            };
        }

        private static List<object> Assign(NavigationMetadata navigation, IReadOnlyList<object> parents, List<IncludedRow> rows)
        {
            List<object> loaded = new List<object>(rows.Count);
            if (navigation.Kind == NavigationKind.Reference)
            {
                Dictionary<object, object> byKey = new Dictionary<object, object>();
                foreach (IncludedRow row in rows)
                {
                    if (row.OwnerKey != null && !byKey.ContainsKey(row.OwnerKey))
                    {
                        byKey[row.OwnerKey] = row.Entity;
                        loaded.Add(row.Entity);
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
            foreach (IncludedRow row in rows)
            {
                if (row.OwnerKey == null) continue;
                if (!groups.TryGetValue(row.OwnerKey, out List<object>? group))
                {
                    group = new List<object>();
                    groups[row.OwnerKey] = group;
                }

                group.Add(row.Entity);
                loaded.Add(row.Entity);
            }

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

            return loaded;
        }

        #endregion
    }
}
