namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Query;
    using Durable.ConcurrencyConflictResolvers;

    /// <summary>
    /// The SQL repository shared by every SQL provider. Providers derive from it and supply a dialect, a connection factory
    /// and the ADO.NET connection type; they may override bulk insert and database creation.
    /// All statements are parameterized. Metadata, materializers and accessors are cached per type.
    /// Thread safety: safe for concurrent use. Configuration members (query filters, <see cref="ConflictResolver"/>,
    /// <see cref="CaptureSql"/>) should be set before concurrent use.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public abstract class SqlRepository<T> : ISqlRepository<T> where T : class, new()
    {
        #region Public-Members

        /// <inheritdoc />
        public EntityMetadata Metadata { get; }

        /// <inheritdoc />
        public RepositoryCapabilities Capabilities => RepositoryCapabilities.All;

        /// <inheritdoc />
        public ISqlDialect Dialect { get; }

        /// <inheritdoc />
        public IConnectionFactory ConnectionFactory { get; }

        /// <inheritdoc />
        public SqlRepositoryOptions Options { get; }

        /// <inheritdoc />
        public RepositorySettings? Settings { get; }

        /// <summary>
        /// Gets the converter in use (the options' converter, or the dialect's). Never null.
        /// </summary>
        public IDataTypeConverter Converter { get; }

        /// <summary>
        /// Gets the command executor. Never null.
        /// </summary>
        public SqlCommandExecutor Executor { get; }

        /// <inheritdoc />
        public IReadOnlyList<Expression<Func<T, bool>>> QueryFilters => _QueryFilters;

        /// <inheritdoc />
        public IConcurrencyConflictResolver<T> ConflictResolver
        {
            get => _ConflictResolver;
            set => _ConflictResolver = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <inheritdoc />
        public bool CaptureSql { get; set; }

        /// <inheritdoc />
        public bool IncludeQueryInResults { get; set; }

        /// <inheritdoc />
        public string? LastExecutedSql => CurrentCapture()?.Sql;

        /// <inheritdoc />
        public string? LastExecutedSqlWithParameters => CurrentCapture()?.ToDebugString();

        #endregion

        #region Private-Members

        private readonly bool _OwnsConnectionFactory;
        private readonly SqlQueryContext _QueryContext;
        private readonly IReadOnlyList<ColumnMetadata> _InsertColumns;
        private readonly IReadOnlyList<ColumnMetadata> _UpdateColumns;
        private volatile IReadOnlyList<Expression<Func<T, bool>>> _QueryFilters = Array.Empty<Expression<Func<T, bool>>>();
        private IConcurrencyConflictResolver<T> _ConflictResolver = new DefaultConflictResolver<T>(ConflictResolutionStrategy.ThrowException);
        private volatile SqlStatement? _LastStatement;
        private int _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the repository.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="connectionFactory">Connection factory. Must not be null.</param>
        /// <param name="ownsConnectionFactory">Whether disposing the repository disposes the factory. Pass false for shared factories.</param>
        /// <param name="connectionType">ADO.NET connection type of the provider. Must not be null.</param>
        /// <param name="settings">Settings the repository was created from; may be null.</param>
        /// <param name="options">Options; null uses defaults.</param>
        /// <exception cref="ArgumentNullException">Thrown when dialect, connectionFactory or connectionType is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when <typeparamref name="T"/> has no primary key or an invalid mapping.</exception>
        protected SqlRepository(ISqlDialect dialect, IConnectionFactory connectionFactory, bool ownsConnectionFactory, Type connectionType, RepositorySettings? settings, SqlRepositoryOptions? options)
        {
            Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            ArgumentNullException.ThrowIfNull(connectionType);
            _OwnsConnectionFactory = ownsConnectionFactory;
            Settings = settings;
            Options = options ?? new SqlRepositoryOptions();
            CaptureSql = Options.CaptureSql;
            IncludeQueryInResults = Options.IncludeQueryInResults;

            Metadata = EntityMetadata.For(typeof(T));
            Metadata.RequireKey();

            Converter = Options.DataTypeConverter ?? dialect.Converter;
            Executor = new SqlCommandExecutor(dialect, connectionFactory, Options, connectionType, typeof(T), Metadata.TableName, OnExecuted);
            _QueryContext = new SqlQueryContext(Executor, Converter);
            _InsertColumns = Metadata.Columns.Where(c => !c.IsAutoIncrement).ToList();
            _UpdateColumns = Metadata.Columns.Where(c => !c.IsPrimaryKey && !c.IsAutoIncrement).ToList();
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public void AddQueryFilter(Expression<Func<T, bool>> filter)
        {
            ArgumentNullException.ThrowIfNull(filter);
            List<Expression<Func<T, bool>>> updated = new List<Expression<Func<T, bool>>>(_QueryFilters) { filter };
            _QueryFilters = updated;
        }

        /// <inheritdoc />
        public void ClearQueryFilters()
        {
            _QueryFilters = Array.Empty<Expression<Func<T, bool>>>();
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> Query(ITransaction? transaction = null)
        {
            ThrowIfDisposed();
            return NewQuery(transaction);
        }

        /// <inheritdoc />
        public ISqlTransaction BeginTransaction()
        {
            ThrowIfDisposed();
            DbConnection connection = ConnectionFactory.OpenConnection();
            try
            {
                DbTransaction transaction = connection.BeginTransaction();
                return new SqlTransactionContext(connection, transaction, true, Dialect);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }

        /// <inheritdoc />
        public async Task<ISqlTransaction> BeginTransactionAsync(CancellationToken token = default)
        {
            ThrowIfDisposed();
            DbConnection connection = await ConnectionFactory.OpenConnectionAsync(token).ConfigureAwait(false);
            try
            {
                DbTransaction transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
                return new SqlTransactionContext(connection, transaction, true, Dialect);
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <inheritdoc />
        public T? ReadFirst(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query.Take(1).Execute().FirstOrDefault();
        }

        /// <inheritdoc />
        public T? ReadFirstOrDefault(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return ReadFirst(predicate, transaction);
        }

        /// <inheritdoc />
        public T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            List<T> results = NewQuery(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count == 0) throw new InvalidOperationException("Sequence contains no matching element.");
            if (results.Count > 1) throw new InvalidOperationException("Sequence contains more than one matching element.");
            return results[0];
        }

        /// <inheritdoc />
        public T? ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            List<T> results = NewQuery(transaction).Where(predicate).Take(2).Execute().ToList();
            if (results.Count > 1) throw new InvalidOperationException("Sequence contains more than one matching element.");
            return results.FirstOrDefault();
        }

        /// <inheritdoc />
        public IEnumerable<T> ReadMany(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return Executor.Query(query.BuildStatement(), transaction, "SELECT", query.CreateMapper());
        }

        /// <inheritdoc />
        public IEnumerable<T> ReadAll(ITransaction? transaction = null)
        {
            return ReadMany(null, transaction);
        }

        /// <inheritdoc />
        public T? ReadById(object id, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(id);
            return NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).Take(1).Execute().FirstOrDefault();
        }

        /// <inheritdoc />
        public async Task<T?> ReadFirstAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return (await query.Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <inheritdoc />
        public Task<T?> ReadFirstOrDefaultAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return ReadFirstAsync(predicate, transaction, token);
        }

        /// <inheritdoc />
        public async Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            List<T> results = (await NewQuery(transaction).Where(predicate).Take(2).ExecuteAsync(token).ConfigureAwait(false)).ToList();
            if (results.Count == 0) throw new InvalidOperationException("Sequence contains no matching element.");
            if (results.Count > 1) throw new InvalidOperationException("Sequence contains more than one matching element.");
            return results[0];
        }

        /// <inheritdoc />
        public async Task<T?> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            List<T> results = (await NewQuery(transaction).Where(predicate).Take(2).ExecuteAsync(token).ConfigureAwait(false)).ToList();
            if (results.Count > 1) throw new InvalidOperationException("Sequence contains more than one matching element.");
            return results.FirstOrDefault();
        }

        /// <inheritdoc />
        public IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query.ExecuteAsyncEnumerable(token);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<T> ReadAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            return ReadManyAsync(null, transaction, token);
        }

        /// <inheritdoc />
        public async Task<T?> ReadByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(id);
            return (await NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
        }

        /// <inheritdoc />
        public bool Exists(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).Any();
        }

        /// <inheritdoc />
        public bool ExistsById(object id, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(id);
            return NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).Any();
        }

        /// <inheritdoc />
        public Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).AnyAsync(token);
        }

        /// <inheritdoc />
        public Task<bool> ExistsByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(id);
            return NewQuery(transaction).WhereKey(Metadata.SplitKey(id)).AnyAsync(token);
        }

        /// <inheritdoc />
        public long Count(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query.Count();
        }

        /// <inheritdoc />
        public Task<long> CountAsync(Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query.CountAsync(token);
        }

        /// <inheritdoc />
        public TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return Filtered(predicate, transaction).Max(selector);
        }

        /// <inheritdoc />
        public TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return Filtered(predicate, transaction).Min(selector);
        }

        /// <inheritdoc />
        public decimal Average<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return Filtered(predicate, transaction).Average(selector);
        }

        /// <inheritdoc />
        public decimal Sum<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null)
        {
            return Filtered(predicate, transaction).Sum(selector);
        }

        /// <inheritdoc />
        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Filtered(predicate, transaction).MaxAsync(selector, token);
        }

        /// <inheritdoc />
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Filtered(predicate, transaction).MinAsync(selector, token);
        }

        /// <inheritdoc />
        public Task<decimal> AverageAsync<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Filtered(predicate, transaction).AverageAsync(selector, token);
        }

        /// <inheritdoc />
        public Task<decimal> SumAsync<TProperty>(Expression<Func<T, TProperty>> selector, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Filtered(predicate, transaction).SumAsync(selector, token);
        }

        /// <inheritdoc />
        public T Create(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            PrepareForInsert(entity);
            SqlStatement statement = BuildInsert(new List<T> { entity }, out bool returnsKeys);
            if (!returnsKeys)
            {
                Executor.ExecuteNonQuery(statement, transaction, "INSERT");
                return entity;
            }

            object? key = Executor.ExecuteScalar(statement, transaction, "INSERT");
            AssignGeneratedKey(entity, key);
            return entity;
        }

        /// <inheritdoc />
        public async Task<T> CreateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            PrepareForInsert(entity);
            SqlStatement statement = BuildInsert(new List<T> { entity }, out bool returnsKeys);
            if (!returnsKeys)
            {
                await Executor.ExecuteNonQueryAsync(statement, transaction, "INSERT", token).ConfigureAwait(false);
                return entity;
            }

            object? key = await Executor.ExecuteScalarAsync(statement, transaction, "INSERT", token).ConfigureAwait(false);
            AssignGeneratedKey(entity, key);
            return entity;
        }

        /// <inheritdoc />
        public IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            List<T> list = MaterializeEntities(entities);
            if (list.Count == 0) return list;
            foreach (T entity in list) PrepareForInsert(entity);

            RunInTransaction(transaction, lease =>
            {
                foreach (List<T> chunk in Chunk(list, InsertChunkSize()))
                {
                    SqlStatement statement = BuildInsert(chunk, out bool returnsKeys);
                    if (!returnsKeys)
                    {
                        Executor.ExecuteNonQuery(lease, statement, "INSERT");
                        continue;
                    }

                    List<object?> keys = Executor.ExecuteReader(lease, statement, "INSERT", ReadGeneratedKeys);
                    AssignGeneratedKeys(chunk, keys);
                }

                return 0;
            });

            return list;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            List<T> list = MaterializeEntities(entities);
            if (list.Count == 0) return list;
            foreach (T entity in list) PrepareForInsert(entity);

            await RunInTransactionAsync(transaction, async lease =>
            {
                foreach (List<T> chunk in Chunk(list, InsertChunkSize()))
                {
                    token.ThrowIfCancellationRequested();
                    SqlStatement statement = BuildInsert(chunk, out bool returnsKeys);
                    if (!returnsKeys)
                    {
                        await Executor.ExecuteNonQueryAsync(lease, statement, "INSERT", token).ConfigureAwait(false);
                        continue;
                    }

                    List<object?> keys = await Executor.ExecuteReaderAsync(lease, statement, "INSERT", ReadGeneratedKeysAsync, token).ConfigureAwait(false);
                    AssignGeneratedKeys(chunk, keys);
                }

                return 0;
            }, token).ConfigureAwait(false);

            return list;
        }

        /// <inheritdoc />
        public T Update(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            object?[] key = RequireKeyValues(entity);
            object? originalVersion = Metadata.VersionColumn?.GetValue(entity);
            SqlStatement statement = BuildUpdate(entity, key, originalVersion, out object? newVersion);
            int rows = Executor.ExecuteNonQuery(statement, transaction, "UPDATE");
            if (rows > 0)
            {
                if (Metadata.VersionColumn != null) Metadata.VersionColumn.SetValue(entity, newVersion);
                return entity;
            }

            if (Metadata.VersionColumn == null)
                throw new InvalidOperationException("No rows were affected during update for entity with key " + FormatKey(key) + ".");

            T? current = ReadByKeyValues(key, transaction);
            T resolved = ResolveConflict(entity, current, key);
            return Update(resolved, transaction);
        }

        /// <inheritdoc />
        public async Task<T> UpdateAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            ThrowIfDisposed();
            object?[] key = RequireKeyValues(entity);
            object? originalVersion = Metadata.VersionColumn?.GetValue(entity);
            SqlStatement statement = BuildUpdate(entity, key, originalVersion, out object? newVersion);
            int rows = await Executor.ExecuteNonQueryAsync(statement, transaction, "UPDATE", token).ConfigureAwait(false);
            if (rows > 0)
            {
                if (Metadata.VersionColumn != null) Metadata.VersionColumn.SetValue(entity, newVersion);
                return entity;
            }

            if (Metadata.VersionColumn == null)
                throw new InvalidOperationException("No rows were affected during update for entity with key " + FormatKey(key) + ".");

            T? current = (await NewQuery(transaction).WhereKey(key).Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
            T resolved = await ResolveConflictAsync(entity, current, key).ConfigureAwait(false);
            return await UpdateAsync(resolved, transaction, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateAction);
            return RunInTransaction(transaction, lease =>
            {
                ITransaction context = lease.TransactionContext!;
                List<T> entities = NewQuery(context).Where(predicate).Execute().ToList();
                foreach (T entity in entities)
                {
                    updateAction(entity);
                    Update(entity, context);
                }

                return entities.Count;
            });
        }

        /// <inheritdoc />
        public Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateAction);
            return RunInTransactionAsync(transaction, async lease =>
            {
                ITransaction context = lease.TransactionContext!;
                List<T> entities = (await NewQuery(context).Where(predicate).ExecuteAsync(token).ConfigureAwait(false)).ToList();
                foreach (T entity in entities)
                {
                    token.ThrowIfCancellationRequested();
                    await updateAction(entity).ConfigureAwait(false);
                    await UpdateAsync(entity, context, token).ConfigureAwait(false);
                }

                return entities.Count;
            }, token);
        }

        /// <inheritdoc />
        public int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null)
        {
            return Executor.ExecuteNonQuery(BuildUpdateField(predicate, field, value), transaction, "UPDATE");
        }

        /// <inheritdoc />
        public Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Executor.ExecuteNonQueryAsync(BuildUpdateField(predicate, field, value), transaction, "UPDATE", token);
        }

        /// <inheritdoc />
        public int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null)
        {
            return Executor.ExecuteNonQuery(BuildBatchUpdate(predicate, updateExpression), transaction, "UPDATE");
        }

        /// <inheritdoc />
        public Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction? transaction = null, CancellationToken token = default)
        {
            return Executor.ExecuteNonQueryAsync(BuildBatchUpdate(predicate, updateExpression), transaction, "UPDATE", token);
        }

        /// <inheritdoc />
        public bool Delete(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return Executor.ExecuteNonQuery(BuildDeleteByKey(RequireKeyValues(entity)), transaction, "DELETE") > 0;
        }

        /// <inheritdoc />
        public bool DeleteById(object id, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(id);
            return Executor.ExecuteNonQuery(BuildDeleteByKey(Metadata.SplitKey(id)), transaction, "DELETE") > 0;
        }

        /// <inheritdoc />
        public int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).Delete();
        }

        /// <inheritdoc />
        public int DeleteAll(ITransaction? transaction = null)
        {
            return NewQuery(transaction).Delete();
        }

        /// <inheritdoc />
        public int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction? transaction = null)
        {
            return DeleteMany(predicate, transaction);
        }

        /// <inheritdoc />
        public async Task<bool> DeleteAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            return await Executor.ExecuteNonQueryAsync(BuildDeleteByKey(RequireKeyValues(entity)), transaction, "DELETE", token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public async Task<bool> DeleteByIdAsync(object id, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(id);
            return await Executor.ExecuteNonQueryAsync(BuildDeleteByKey(Metadata.SplitKey(id)), transaction, "DELETE", token).ConfigureAwait(false) > 0;
        }

        /// <inheritdoc />
        public Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return NewQuery(transaction).Where(predicate).DeleteAsync(token);
        }

        /// <inheritdoc />
        public Task<int> DeleteAllAsync(ITransaction? transaction = null, CancellationToken token = default)
        {
            return NewQuery(transaction).DeleteAsync(token);
        }

        /// <inheritdoc />
        public Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default)
        {
            return DeleteManyAsync(predicate, transaction, token);
        }

        /// <inheritdoc />
        public T Upsert(T entity, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entity);
            if (HasUnsetGeneratedKey(entity)) return Create(entity, transaction);
            return RunInTransaction(transaction, lease =>
            {
                PrepareForInsert(entity);
                Executor.ExecuteNonQuery(lease, BuildUpsert(entity), "UPSERT");
                T? stored = ReadByKeyValues(RequireKeyValues(entity), lease.TransactionContext);
                if (stored != null) CopyColumns(stored, entity);
                return entity;
            });
        }

        /// <inheritdoc />
        public IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            List<T> list = MaterializeEntities(entities);
            if (list.Count == 0) return list;
            RunInTransaction(transaction, lease =>
            {
                foreach (T entity in list) Upsert(entity, lease.TransactionContext);
                return 0;
            });
            return list;
        }

        /// <inheritdoc />
        public async Task<T> UpsertAsync(T entity, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entity);
            if (HasUnsetGeneratedKey(entity)) return await CreateAsync(entity, transaction, token).ConfigureAwait(false);
            return await RunInTransactionAsync(transaction, async lease =>
            {
                PrepareForInsert(entity);
                await Executor.ExecuteNonQueryAsync(lease, BuildUpsert(entity), "UPSERT", token).ConfigureAwait(false);
                T? stored = (await NewQuery(lease.TransactionContext).WhereKey(RequireKeyValues(entity)).Take(1).ExecuteAsync(token).ConfigureAwait(false)).FirstOrDefault();
                if (stored != null) CopyColumns(stored, entity);
                return entity;
            }, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            List<T> list = MaterializeEntities(entities);
            if (list.Count == 0) return list;
            await RunInTransactionAsync(transaction, async lease =>
            {
                foreach (T entity in list)
                {
                    token.ThrowIfCancellationRequested();
                    await UpsertAsync(entity, lease.TransactionContext, token).ConfigureAwait(false);
                }

                return 0;
            }, token).ConfigureAwait(false);
            return list;
        }

        /// <inheritdoc />
        public IEnumerable<T> FromSql(string sql, ITransaction? transaction = null, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Executor.Query(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW", ResultMapper.Create<T>(Converter));
        }

        /// <inheritdoc />
        public IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction? transaction = null, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Executor.Query(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW", ResultMapper.Create<TResult>(Converter));
        }

        /// <inheritdoc />
        public int ExecuteSql(string sql, ITransaction? transaction = null, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Executor.ExecuteNonQuery(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW");
        }

        /// <inheritdoc />
        public TResult? ExecuteScalar<TResult>(string sql, ITransaction? transaction = null, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            object? value = Executor.ExecuteScalar(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW");
            return value == null ? default : (TResult?)Converter.ConvertFromDatabase(value, typeof(TResult));
        }

        /// <inheritdoc />
        public IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Executor.QueryAsync(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW", ResultMapper.Create<T>(Converter), token);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Executor.QueryAsync(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW", ResultMapper.Create<TResult>(Converter), token);
        }

        /// <inheritdoc />
        public Task<int> ExecuteSqlAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Executor.ExecuteNonQueryAsync(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW", token);
        }

        /// <inheritdoc />
        public async Task<TResult?> ExecuteScalarAsync<TResult>(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            object? value = await Executor.ExecuteScalarAsync(RawSql.Positional(sql, parameters, Dialect, Converter), transaction, "RAW", token).ConfigureAwait(false);
            return value == null ? default : (TResult?)Converter.ConvertFromDatabase(value, typeof(TResult));
        }

        /// <inheritdoc />
        public SqlMultipleResultReader QueryMultiple(string sql, ITransaction? transaction = null, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            SqlStatement statement = RawSql.Positional(sql, parameters, Dialect, Converter);
            ConnectionLease lease = Executor.Lease(transaction);
            try
            {
                return Executor.ExecuteMultiple(lease, statement, Converter);
            }
            catch
            {
                lease.Dispose();
                throw;
            }
        }

        /// <inheritdoc />
        public async Task<SqlMultipleResultReader> QueryMultipleAsync(string sql, ITransaction? transaction = null, CancellationToken token = default, params object?[] parameters)
        {
            ArgumentNullException.ThrowIfNull(sql);
            SqlStatement statement = RawSql.Positional(sql, parameters, Dialect, Converter);
            ConnectionLease lease = await Executor.LeaseAsync(transaction, token).ConfigureAwait(false);
            try
            {
                return await Executor.ExecuteMultipleAsync(lease, statement, Converter, token).ConfigureAwait(false);
            }
            catch
            {
                await lease.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <inheritdoc />
        public int ExecuteProcedure(string procedureName, ITransaction? transaction = null, params SqlParameterValue[] parameters)
        {
            return Executor.ExecuteNonQuery(ProcedureStatement(procedureName, parameters), transaction, "PROCEDURE", CommandType.StoredProcedure);
        }

        /// <inheritdoc />
        public List<TResult> FromProcedure<TResult>(string procedureName, ITransaction? transaction = null, params SqlParameterValue[] parameters)
        {
            SqlStatement statement = ProcedureStatement(procedureName, parameters);
            Func<DbDataReader, TResult> map = ResultMapper.Create<TResult>(Converter);
            return Executor.ExecuteReader(statement, transaction, "PROCEDURE", reader =>
            {
                List<TResult> rows = new List<TResult>();
                while (reader.Read()) rows.Add(map(reader));
                return rows;
            }, CommandType.StoredProcedure);
        }

        /// <inheritdoc />
        public Task<int> ExecuteProcedureAsync(string procedureName, ITransaction? transaction = null, CancellationToken token = default, params SqlParameterValue[] parameters)
        {
            return Executor.ExecuteNonQueryAsync(ProcedureStatement(procedureName, parameters), transaction, "PROCEDURE", token, CommandType.StoredProcedure);
        }

        /// <inheritdoc />
        public Task<List<TResult>> FromProcedureAsync<TResult>(string procedureName, ITransaction? transaction = null, CancellationToken token = default, params SqlParameterValue[] parameters)
        {
            SqlStatement statement = ProcedureStatement(procedureName, parameters);
            Func<DbDataReader, TResult> map = ResultMapper.Create<TResult>(Converter);
            return Executor.ExecuteReaderAsync(statement, transaction, "PROCEDURE", async (reader, ct) =>
            {
                List<TResult> rows = new List<TResult>();
                while (await reader.ReadAsync(ct).ConfigureAwait(false)) rows.Add(map(reader));
                return rows;
            }, token, CommandType.StoredProcedure);
        }

        /// <inheritdoc />
        public long BulkInsert(IEnumerable<T> entities, ITransaction? transaction = null)
        {
            List<T> list = MaterializeEntities(entities);
            if (list.Count == 0) return 0;
            foreach (T entity in list) PrepareForInsert(entity);
            return RunInTransaction(transaction, lease => BulkInsertCore(lease, list));
        }

        /// <inheritdoc />
        public async Task<long> BulkInsertAsync(IEnumerable<T> entities, ITransaction? transaction = null, CancellationToken token = default)
        {
            List<T> list = MaterializeEntities(entities);
            if (list.Count == 0) return 0;
            foreach (T entity in list) PrepareForInsert(entity);
            return await RunInTransactionAsync(transaction, lease => BulkInsertCoreAsync(lease, list, token), token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void InitializeTable(Type entityType, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            EntityMetadata metadata = EntityMetadata.For(entityType);
            List<string> errors = ValidateMapping(metadata);
            if (errors.Count > 0) throw new InvalidOperationException("Table validation failed:\n" + string.Join("\n", errors));

            if (!TableExists(metadata.TableName, transaction))
            {
                SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
                Dialect.AppendCreateTable(builder, metadata);
                Executor.ExecuteNonQuery(builder.Build(), transaction, "DDL");
            }
            else
            {
                List<string> columnErrors = CompareColumns(metadata, GetColumnNames(metadata.TableName, transaction), new List<string>());
                if (columnErrors.Count > 0) throw new InvalidOperationException("Table validation failed:\n" + string.Join("\n", columnErrors));
            }

            CreateIndexes(entityType, transaction);
        }

        /// <inheritdoc />
        public async Task InitializeTableAsync(Type entityType, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            EntityMetadata metadata = EntityMetadata.For(entityType);
            List<string> errors = ValidateMapping(metadata);
            if (errors.Count > 0) throw new InvalidOperationException("Table validation failed:\n" + string.Join("\n", errors));

            if (!await TableExistsAsync(metadata.TableName, transaction, token).ConfigureAwait(false))
            {
                SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
                Dialect.AppendCreateTable(builder, metadata);
                await Executor.ExecuteNonQueryAsync(builder.Build(), transaction, "DDL", token).ConfigureAwait(false);
            }
            else
            {
                List<string> columns = await GetColumnNamesAsync(metadata.TableName, transaction, token).ConfigureAwait(false);
                List<string> columnErrors = CompareColumns(metadata, columns, new List<string>());
                if (columnErrors.Count > 0) throw new InvalidOperationException("Table validation failed:\n" + string.Join("\n", columnErrors));
            }

            await CreateIndexesAsync(entityType, transaction, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void InitializeTables(IEnumerable<Type> entityTypes, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            foreach (Type type in entityTypes) InitializeTable(type, transaction);
        }

        /// <inheritdoc />
        public async Task InitializeTablesAsync(IEnumerable<Type> entityTypes, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            foreach (Type type in entityTypes) await InitializeTableAsync(type, transaction, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public bool ValidateTable(Type entityType, out List<string> errors, out List<string> warnings)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            warnings = new List<string>();
            EntityMetadata metadata;
            try
            {
                metadata = EntityMetadata.For(entityType);
            }
            catch (InvalidOperationException e)
            {
                errors = new List<string> { e.Message };
                return false;
            }

            errors = ValidateMapping(metadata);
            if (errors.Count == 0 && TableExists(metadata.TableName, null))
                errors.AddRange(CompareColumns(metadata, GetColumnNames(metadata.TableName, null), warnings));
            return errors.Count == 0;
        }

        /// <inheritdoc />
        public bool ValidateTables(IEnumerable<Type> entityTypes, out List<string> errors, out List<string> warnings)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            errors = new List<string>();
            warnings = new List<string>();
            foreach (Type type in entityTypes)
            {
                ValidateTable(type, out List<string> typeErrors, out List<string> typeWarnings);
                errors.AddRange(typeErrors.Select(e => type.Name + ": " + e));
                warnings.AddRange(typeWarnings.Select(w => type.Name + ": " + w));
            }

            return errors.Count == 0;
        }

        /// <inheritdoc />
        public void CreateIndexes(Type entityType, ITransaction? transaction = null)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            EntityMetadata metadata = EntityMetadata.For(entityType);
            HashSet<string> existing = new HashSet<string>(GetIndexNames(metadata.TableName, transaction), StringComparer.OrdinalIgnoreCase);
            foreach (string sql in IndexStatements(metadata, existing)) Executor.ExecuteNonQuery(new SqlStatement(sql), transaction, "DDL");
        }

        /// <inheritdoc />
        public async Task CreateIndexesAsync(Type entityType, ITransaction? transaction = null, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            EntityMetadata metadata = EntityMetadata.For(entityType);
            HashSet<string> existing = new HashSet<string>(await GetIndexNamesAsync(metadata.TableName, transaction, token).ConfigureAwait(false), StringComparer.OrdinalIgnoreCase);
            foreach (string sql in IndexStatements(metadata, existing))
                await Executor.ExecuteNonQueryAsync(new SqlStatement(sql), transaction, "DDL", token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public void DropIndex(string indexName, ITransaction? transaction = null)
        {
            if (string.IsNullOrWhiteSpace(indexName)) throw new ArgumentNullException(nameof(indexName));
            Executor.ExecuteNonQuery(new SqlStatement(Dialect.DropIndexSql(indexName, Metadata.TableName)), transaction, "DDL");
        }

        /// <inheritdoc />
        public Task DropIndexAsync(string indexName, ITransaction? transaction = null, CancellationToken token = default)
        {
            if (string.IsNullOrWhiteSpace(indexName)) throw new ArgumentNullException(nameof(indexName));
            return Executor.ExecuteNonQueryAsync(new SqlStatement(Dialect.DropIndexSql(indexName, Metadata.TableName)), transaction, "DDL", token);
        }

        /// <inheritdoc />
        public List<string> GetIndexes(Type entityType)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return GetIndexNames(EntityMetadata.For(entityType).TableName, null);
        }

        /// <inheritdoc />
        public Task<List<string>> GetIndexesAsync(Type entityType, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            return GetIndexNamesAsync(EntityMetadata.For(entityType).TableName, null, token);
        }

        /// <inheritdoc />
        public virtual void CreateDatabaseIfNotExists()
        {
        }

        /// <inheritdoc />
        public virtual Task CreateDatabaseIfNotExistsAsync(CancellationToken token = default)
        {
            CreateDatabaseIfNotExists();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Disposes the repository and, when it created its connection factory, the factory.
        /// Repositories constructed with a caller-supplied factory leave it untouched.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        IQueryBuilder<T> IRepository<T>.Query(ITransaction? transaction) => Query(transaction);

        ITransaction IRepository<T>.BeginTransaction() => BeginTransaction();

        async Task<ITransaction> IRepository<T>.BeginTransactionAsync(CancellationToken token) => await BeginTransactionAsync(token).ConfigureAwait(false);

        #endregion

        #region Private-Methods

        /// <summary>
        /// Inserts rows using the fastest path the database offers. The default uses batched multi-row INSERT statements.
        /// Generated keys are not read back.
        /// </summary>
        /// <param name="lease">Lease inside a transaction. Must not be null.</param>
        /// <param name="entities">Prepared entities. Must not be null.</param>
        /// <returns>Rows inserted.</returns>
        protected virtual long BulkInsertCore(ConnectionLease lease, IReadOnlyList<T> entities)
        {
            long total = 0;
            foreach (List<T> chunk in Chunk(entities, InsertChunkSize()))
                total += Executor.ExecuteNonQuery(lease, BuildMultiRowInsert(chunk), "BULK INSERT");
            return total;
        }

        /// <summary>
        /// Inserts rows using the fastest path the database offers. The default uses batched multi-row INSERT statements.
        /// </summary>
        /// <param name="lease">Lease inside a transaction. Must not be null.</param>
        /// <param name="entities">Prepared entities. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Rows inserted.</returns>
        protected virtual async Task<long> BulkInsertCoreAsync(ConnectionLease lease, IReadOnlyList<T> entities, CancellationToken token)
        {
            long total = 0;
            foreach (List<T> chunk in Chunk(entities, InsertChunkSize()))
            {
                token.ThrowIfCancellationRequested();
                total += await Executor.ExecuteNonQueryAsync(lease, BuildMultiRowInsert(chunk), "BULK INSERT", token).ConfigureAwait(false);
            }

            return total;
        }

        /// <summary>
        /// Gets the columns written on insert (all except auto-increment). Never null.
        /// </summary>
        protected IReadOnlyList<ColumnMetadata> InsertColumns => _InsertColumns;

        /// <summary>
        /// Returns the database value for a column of an entity, applying converters.
        /// </summary>
        /// <param name="entity">Entity. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <returns>The database value (never null; <see cref="DBNull.Value"/> for null).</returns>
        protected object DatabaseValue(T entity, ColumnMetadata column)
        {
            return Converter.ConvertToDatabase(column.GetValue(entity), column);
        }

        /// <summary>
        /// Releases resources.
        /// </summary>
        /// <param name="disposing">True when called from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _Disposed, 1) == 1) return;
            if (disposing && _OwnsConnectionFactory) ConnectionFactory.Dispose();
        }

        /// <summary>
        /// Throws when the repository is disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when disposed.</exception>
        protected void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) == 1) throw new ObjectDisposedException(GetType().Name);
        }


        private SqlQueryBuilder<T> NewQuery(ITransaction? transaction)
        {
            return new SqlQueryBuilder<T>(_QueryContext, transaction, _QueryFilters);
        }

        private SqlQueryBuilder<T> Filtered(Expression<Func<T, bool>>? predicate, ITransaction? transaction)
        {
            SqlQueryBuilder<T> query = NewQuery(transaction);
            if (predicate != null) query.Where(predicate);
            return query;
        }

        private void OnExecuted(SqlStatement statement)
        {
            if (CaptureSql) _LastStatement = statement;
            SqlCaptureScope.Current?.Record(statement);
        }

        private SqlStatement? CurrentCapture()
        {
            return SqlCaptureScope.Current?.LastStatement ?? _LastStatement;
        }

        private static List<T> MaterializeEntities(IEnumerable<T> entities)
        {
            ArgumentNullException.ThrowIfNull(entities);
            List<T> list = entities as List<T> ?? entities.ToList();
            for (int i = 0; i < list.Count; i++)
            {
                if (list[i] == null) throw new ArgumentNullException(nameof(entities), "Entity at index " + i + " is null.");
            }

            return list;
        }

        private void PrepareForInsert(T entity)
        {
            foreach (ColumnMetadata column in Metadata.Columns)
            {
                DefaultValueProviderInfo? defaultValue = column.DefaultValue;
                if (defaultValue == null) continue;
                object? current = column.GetValue(entity);
                if (defaultValue.Provider.ShouldApply(current, column.PropertyType))
                    column.SetValue(entity, defaultValue.Provider.GetDefaultValue(column.Property, entity));
            }

            ColumnMetadata? version = Metadata.VersionColumn;
            if (version != null)
            {
                object? value = version.GetValue(entity);
                if (IsUnsetVersion(value)) version.SetValue(entity, Metadata.VersionInfo!.GetDefaultVersion());
            }
        }

        private static bool IsUnsetVersion(object? value)
        {
            switch (value)
            {
                case null: return true;
                case int i: return i == 0;
                case long l: return l == 0;
                case short s: return s == 0;
                case byte b: return b == 0;
                case DateTime dt: return dt == DateTime.MinValue;
                case Guid g: return g == Guid.Empty;
                case byte[] bytes: return bytes.Length == 0;
                default: return false;
            }
        }

        private int InsertChunkSize()
        {
            IBatchInsertConfiguration config = Options.BatchConfiguration;
            int maxParameters = Math.Min(config.MaxParametersPerStatement, Dialect.MaxParameters);
            int perRow = Math.Max(1, _InsertColumns.Count);
            int byParameters = Math.Max(1, maxParameters / perRow);
            int rows = config.EnableMultiRowInsert ? Math.Min(config.MaxRowsPerBatch, byParameters) : Math.Min(config.MaxRowsPerBatch, byParameters);
            return Math.Max(1, rows);
        }

        private static IEnumerable<List<T>> Chunk(IReadOnlyList<T> source, int size)
        {
            for (int offset = 0; offset < source.Count; offset += size)
            {
                List<T> chunk = new List<T>(Math.Min(size, source.Count - offset));
                for (int i = offset; i < offset + size && i < source.Count; i++) chunk.Add(source[i]);
                yield return chunk;
            }
        }

        private SqlStatement BuildInsert(List<T> rows, out bool returnsKeys)
        {
            ColumnMetadata? generated = Metadata.AutoIncrementColumn;
            returnsKeys = generated != null;
            if (!returnsKeys && Options.BatchConfiguration.EnableMultiRowInsert) return BuildMultiRowInsert(rows);

            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            for (int r = 0; r < rows.Count; r++)
            {
                if (r > 0) builder.Append(Dialect.StatementSeparator).Append(" ");
                AppendSingleInsert(builder, rows[r], generated);
            }

            return builder.Build();
        }

        private void AppendSingleInsert(SqlStatementBuilder builder, T entity, ColumnMetadata? generated)
        {
            string table = Dialect.QuoteIdentifier(Metadata.TableName);
            string? output = generated != null && Dialect.InsertKeyStrategy == InsertKeyStrategy.Output ? " OUTPUT INSERTED." + Dialect.QuoteIdentifier(generated.Name) : null;
            builder.Append("INSERT INTO ").Append(table);
            if (_InsertColumns.Count == 0)
            {
                builder.Append(output).Append(" ").Append(Dialect.InsertDefaultValuesClause);
            }
            else
            {
                builder.Append(" (").Append(string.Join(", ", _InsertColumns.Select(c => Dialect.QuoteIdentifier(c.Name)))).Append(")").Append(output).Append(" VALUES (");
                for (int i = 0; i < _InsertColumns.Count; i++)
                {
                    if (i > 0) builder.Append(", ");
                    builder.AppendParameter(DatabaseValue(entity, _InsertColumns[i]), _InsertColumns[i]);
                }

                builder.Append(")");
            }

            if (generated == null) return;
            if (Dialect.InsertKeyStrategy == InsertKeyStrategy.Returning)
                builder.Append(" RETURNING ").AppendIdentifier(generated.Name);
            else if (Dialect.InsertKeyStrategy == InsertKeyStrategy.LastInsertId)
                builder.Append(Dialect.StatementSeparator).Append(" ").Append(Dialect.LastInsertIdSql);
        }

        private SqlStatement BuildMultiRowInsert(IReadOnlyList<T> rows)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            if (_InsertColumns.Count == 0)
            {
                for (int r = 0; r < rows.Count; r++)
                {
                    if (r > 0) builder.Append(Dialect.StatementSeparator).Append(" ");
                    builder.Append("INSERT INTO ").AppendIdentifier(Metadata.TableName).Append(" ").Append(Dialect.InsertDefaultValuesClause);
                }

                return builder.Build();
            }

            builder.Append("INSERT INTO ").AppendIdentifier(Metadata.TableName).Append(" (")
                .Append(string.Join(", ", _InsertColumns.Select(c => Dialect.QuoteIdentifier(c.Name)))).Append(") VALUES ");
            for (int r = 0; r < rows.Count; r++)
            {
                if (r > 0) builder.Append(", ");
                builder.Append("(");
                for (int i = 0; i < _InsertColumns.Count; i++)
                {
                    if (i > 0) builder.Append(", ");
                    builder.AppendParameter(DatabaseValue(rows[r], _InsertColumns[i]), _InsertColumns[i]);
                }

                builder.Append(")");
            }

            return builder.Build();
        }

        private static List<object?> ReadGeneratedKeys(DbDataReader reader)
        {
            List<object?> keys = new List<object?>();
            do
            {
                if (reader.FieldCount == 0) continue;
                while (reader.Read()) keys.Add(reader.IsDBNull(0) ? null : reader.GetValue(0));
            }
            while (reader.NextResult());
            return keys;
        }

        private static async Task<List<object?>> ReadGeneratedKeysAsync(DbDataReader reader, CancellationToken token)
        {
            List<object?> keys = new List<object?>();
            do
            {
                if (reader.FieldCount == 0) continue;
                while (await reader.ReadAsync(token).ConfigureAwait(false)) keys.Add(reader.IsDBNull(0) ? null : reader.GetValue(0));
            }
            while (await reader.NextResultAsync(token).ConfigureAwait(false));
            return keys;
        }

        private void AssignGeneratedKey(T entity, object? key)
        {
            ColumnMetadata? generated = Metadata.AutoIncrementColumn;
            if (generated == null || key == null) return;
            generated.SetValue(entity, Converter.ConvertFromDatabase(key, generated.PropertyType, generated));
        }

        private void AssignGeneratedKeys(List<T> chunk, List<object?> keys)
        {
            if (keys.Count != chunk.Count)
                throw new InvalidOperationException("Expected " + chunk.Count + " generated keys but the database returned " + keys.Count + ".");
            for (int i = 0; i < chunk.Count; i++) AssignGeneratedKey(chunk[i], keys[i]);
        }

        private bool HasUnsetGeneratedKey(T entity)
        {
            ColumnMetadata? generated = Metadata.AutoIncrementColumn;
            if (generated == null || !generated.IsPrimaryKey) return false;
            object? value = generated.GetValue(entity);
            return value == null || IsUnsetVersion(value) || (value is string s && s.Length == 0);
        }

        private object?[] RequireKeyValues(T entity)
        {
            object?[] values = new object?[Metadata.KeyColumns.Count];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = Metadata.KeyColumns[i].GetValue(entity);
                if (values[i] == null)
                    throw new InvalidOperationException("Primary key column '" + Metadata.KeyColumns[i].Name + "' of " + typeof(T).Name + " is null.");
            }

            return values;
        }

        private static string FormatKey(object?[] key)
        {
            return key.Length == 1 ? Convert.ToString(key[0], CultureInfo.InvariantCulture) ?? "null" : "(" + string.Join(", ", key.Select(k => Convert.ToString(k, CultureInfo.InvariantCulture))) + ")";
        }

        private TableSource TableQualifiedSource()
        {
            return new TableSource(Dialect.QuoteIdentifier(Metadata.TableName), Metadata);
        }

        private SqlStatement BuildUpdate(T entity, object?[] key, object? originalVersion, out object? newVersion)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            SqlExpressionTranslator translator = new SqlExpressionTranslator(builder, Converter, Options.StringMatching);
            TableSource source = TableQualifiedSource();
            ColumnMetadata? version = Metadata.VersionColumn;
            newVersion = version != null ? Metadata.VersionInfo!.IncrementVersion(originalVersion!) : null;

            builder.Append("UPDATE ").AppendIdentifier(Metadata.TableName).Append(" SET ");
            bool first = true;
            foreach (ColumnMetadata column in _UpdateColumns)
            {
                if (!first) builder.Append(", ");
                first = false;
                object? value = column.IsVersion ? newVersion : column.GetValue(entity);
                builder.AppendIdentifier(column.Name).Append(" = ").AppendParameter(Converter.ConvertToDatabase(value, column), column);
            }

            if (first) throw new InvalidOperationException("Entity " + typeof(T).Name + " has no updatable columns.");

            builder.Append(" WHERE ").Append(SqlQueryBuilder<T>.KeyCondition(translator, source, Metadata, key));
            if (version != null)
            {
                builder.Append(" AND ");
                if (originalVersion == null) builder.Append(translator.ColumnSql(source, version)).Append(" IS NULL");
                else builder.Append(translator.ColumnSql(source, version)).Append(" = ").Append(translator.Parameter(originalVersion, version));
            }

            return builder.Build();
        }

        private T ResolveConflict(T incoming, T? current, object?[] key)
        {
            if (current == null)
                throw new OptimisticConcurrencyException("Entity " + typeof(T).Name + " with key " + FormatKey(key) + " was deleted by another process.");

            T original = CreateOriginalApproximation(current, incoming);
            bool resolved;
            T result;
            try
            {
                resolved = _ConflictResolver.TryResolveConflict(current, incoming, original, _ConflictResolver.DefaultStrategy, out result);
            }
            catch (OptimisticConcurrencyException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new OptimisticConcurrencyException("Error during conflict resolution for " + typeof(T).Name + " with key " + FormatKey(key) + ": " + e.Message, e);
            }

            if (!resolved || result == null)
                throw new OptimisticConcurrencyException("Optimistic concurrency conflict detected for " + typeof(T).Name + " with key " + FormatKey(key) + " and it could not be resolved.");

            Metadata.VersionColumn!.SetValue(result, Metadata.VersionColumn.GetValue(current));
            return result;
        }

        private async Task<T> ResolveConflictAsync(T incoming, T? current, object?[] key)
        {
            if (current == null)
                throw new OptimisticConcurrencyException("Entity " + typeof(T).Name + " with key " + FormatKey(key) + " was deleted by another process.");

            T original = CreateOriginalApproximation(current, incoming);
            TryResolveConflictResult<T> outcome;
            try
            {
                outcome = await _ConflictResolver.TryResolveConflictAsync(current, incoming, original, _ConflictResolver.DefaultStrategy).ConfigureAwait(false);
            }
            catch (OptimisticConcurrencyException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new OptimisticConcurrencyException("Error during conflict resolution for " + typeof(T).Name + " with key " + FormatKey(key) + ": " + e.Message, e);
            }

            if (!outcome.Success || outcome.ResolvedEntity == null)
                throw new OptimisticConcurrencyException("Optimistic concurrency conflict detected for " + typeof(T).Name + " with key " + FormatKey(key) + " and it could not be resolved.");

            Metadata.VersionColumn!.SetValue(outcome.ResolvedEntity, Metadata.VersionColumn.GetValue(current));
            return outcome.ResolvedEntity;
        }

        private T CreateOriginalApproximation(T current, T incoming)
        {
            // Without change tracking the values the client originally read are unknown; the incoming entity is the
            // closest available approximation, so merge resolvers keep the stored value for every column.
            T original = new T();
            foreach (ColumnMetadata column in Metadata.Columns) column.SetValue(original, column.GetValue(incoming));
            return original;
        }

        private T? ReadByKeyValues(object?[] key, ITransaction? transaction)
        {
            return NewQuery(transaction).WhereKey(key).Take(1).Execute().FirstOrDefault();
        }

        private void CopyColumns(T source, T target)
        {
            foreach (ColumnMetadata column in Metadata.Columns) column.SetValue(target, column.GetValue(source));
        }

        private SqlStatement BuildUpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(field);
            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            SqlExpressionTranslator translator = new SqlExpressionTranslator(builder, Converter, Options.StringMatching);
            TableSource source = TableQualifiedSource();
            translator.Bind(field.Parameters[0], source);
            ColumnReference reference = translator.ResolveColumn(field.Body)
                ?? throw new ArgumentException("The field selector must reference a mapped column, for example x => x.Name.", nameof(field));

            List<string> assignments = new List<string>
            {
                Dialect.QuoteIdentifier(reference.Column.Name) + " = " + translator.Parameter(value, reference.Column)
            };
            AppendVersionBump(assignments, translator, source, reference.Column);
            return BuildBulkUpdate(builder, translator, source, predicate, assignments);
        }

        private SqlStatement BuildBatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(updateExpression);
            if (updateExpression.Body is not MemberInitExpression memberInit)
                throw new NotSupportedException("BatchUpdate requires a member-init expression, for example x => new T { Name = \"value\" }.");

            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            SqlExpressionTranslator translator = new SqlExpressionTranslator(builder, Converter, Options.StringMatching);
            TableSource source = TableQualifiedSource();
            translator.Bind(updateExpression.Parameters[0], source);

            List<string> assignments = new List<string>();
            ColumnMetadata? explicitVersion = null;
            foreach (MemberBinding binding in memberInit.Bindings)
            {
                if (binding is not MemberAssignment assignment || assignment.Member is not PropertyInfo property)
                    throw new NotSupportedException("BatchUpdate bindings must assign properties.");
                ColumnMetadata column = Metadata.FindColumn(property)
                    ?? throw new NotSupportedException("Property '" + property.Name + "' is not a mapped column.");
                if (column.IsPrimaryKey) throw new NotSupportedException("BatchUpdate cannot modify primary key column '" + column.Name + "'.");
                if (column.IsVersion) explicitVersion = column;

                string valueSql = ExpressionEvaluator.IsEvaluable(assignment.Expression)
                    ? translator.Parameter(ExpressionEvaluator.Evaluate(assignment.Expression), column)
                    : translator.Value(assignment.Expression);
                assignments.Add(Dialect.QuoteIdentifier(column.Name) + " = " + valueSql);
            }

            if (assignments.Count == 0) throw new NotSupportedException("BatchUpdate requires at least one assignment.");
            if (explicitVersion == null) AppendVersionBump(assignments, translator, source, null);
            return BuildBulkUpdate(builder, translator, source, predicate, assignments);
        }

        private void AppendVersionBump(List<string> assignments, SqlExpressionTranslator translator, TableSource source, ColumnMetadata? assignedColumn)
        {
            ColumnMetadata? version = Metadata.VersionColumn;
            if (version == null || ReferenceEquals(version, assignedColumn)) return;
            switch (Metadata.VersionInfo!.Type)
            {
                case VersionColumnType.Integer:
                    assignments.Add(Dialect.QuoteIdentifier(version.Name) + " = " + translator.ColumnSql(source, version) + " + 1");
                    break;
                case VersionColumnType.Timestamp:
                    assignments.Add(Dialect.QuoteIdentifier(version.Name) + " = " + translator.Parameter(DateTime.UtcNow, version));
                    break;
                case VersionColumnType.Guid:
                    assignments.Add(Dialect.QuoteIdentifier(version.Name) + " = " + translator.Parameter(Guid.NewGuid(), version));
                    break;
            }
        }

        private SqlStatement BuildBulkUpdate(SqlStatementBuilder builder, SqlExpressionTranslator translator, TableSource source, Expression<Func<T, bool>> predicate, List<string> assignments)
        {
            SqlQueryBuilder<T> query = NewQuery(null);
            query.Where(predicate);
            List<string> conditions = query.RenderConditions(translator, source);
            builder.Append("UPDATE ").AppendIdentifier(Metadata.TableName).Append(" SET ").Append(string.Join(", ", assignments));
            if (conditions.Count > 0) builder.Append(" WHERE ").Append(string.Join(" AND ", conditions));
            return builder.Build();
        }

        private SqlStatement BuildDeleteByKey(object?[] key)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            SqlExpressionTranslator translator = new SqlExpressionTranslator(builder, Converter, Options.StringMatching);
            List<string> conditions = new List<string> { SqlQueryBuilder<T>.KeyCondition(translator, TableQualifiedSource(), Metadata, key) };
            SqlWriteBuilder.AppendDelete(builder, Metadata, Converter, conditions);
            return builder.Build();
        }

        private SqlStatement BuildUpsert(T entity)
        {
            SqlStatementBuilder builder = new SqlStatementBuilder(Dialect);
            List<ColumnMetadata> insertColumns = Metadata.Columns.Where(c => !c.IsAutoIncrement || c.IsPrimaryKey).ToList();
            List<string> placeholders = insertColumns.Select(c => builder.AddParameter(DatabaseValue(entity, c), c)).ToList();
            string? versionPlaceholder = null;
            ColumnMetadata? version = Metadata.VersionColumn;
            if (version != null)
            {
                object? next = Metadata.VersionInfo!.IncrementVersion(version.GetValue(entity)!);
                versionPlaceholder = builder.AddParameter(Converter.ConvertToDatabase(next, version), version);
            }

            Dialect.AppendUpsert(builder, Metadata, insertColumns, placeholders, Metadata.KeyColumns, _UpdateColumns, versionPlaceholder);
            return builder.Build();
        }

        private SqlStatement ProcedureStatement(string procedureName, SqlParameterValue[] parameters)
        {
            if (string.IsNullOrWhiteSpace(procedureName)) throw new ArgumentNullException(nameof(procedureName));
            if (!Dialect.SupportsStoredProcedures)
                throw new NotSupportedException(Dialect.RepositoryType.DisplayName + " does not support stored procedures.");
            List<SqlParameterValue> list = new List<SqlParameterValue>();
            if (parameters != null)
            {
                foreach (SqlParameterValue parameter in parameters)
                {
                    ArgumentNullException.ThrowIfNull(parameter);
                    if (parameter.Direction == ParameterDirection.Input && parameter.Value != null && parameter.Value != DBNull.Value)
                        parameter.Value = Converter.ConvertToDatabase(parameter.Value, parameter.Column);
                    list.Add(parameter);
                }
            }

            return new SqlStatement(procedureName, list);
        }

        private TResult RunInTransaction<TResult>(ITransaction? transaction, Func<ConnectionLease, TResult> work)
        {
            ThrowIfDisposed();
            ISqlTransaction? existing = Executor.ResolveTransaction(transaction);
            if (existing != null) return work(new ConnectionLease(existing.Connection, existing));

            using ISqlTransaction owned = BeginTransaction();
            TResult result = work(new ConnectionLease(owned.Connection, owned));
            owned.Commit();
            return result;
        }

        private async Task<TResult> RunInTransactionAsync<TResult>(ITransaction? transaction, Func<ConnectionLease, Task<TResult>> work, CancellationToken token)
        {
            ThrowIfDisposed();
            ISqlTransaction? existing = Executor.ResolveTransaction(transaction);
            if (existing != null) return await work(new ConnectionLease(existing.Connection, existing)).ConfigureAwait(false);

            ISqlTransaction owned = await BeginTransactionAsync(token).ConfigureAwait(false);
            await using (owned.ConfigureAwait(false))
            {
                TResult result = await work(new ConnectionLease(owned.Connection, owned)).ConfigureAwait(false);
                await owned.CommitAsync(token).ConfigureAwait(false);
                return result;
            }
        }

        private bool TableExists(string tableName, ITransaction? transaction)
        {
            return Executor.Query(Dialect.TableExistsQuery(tableName), transaction, "DDL", r => 1).Any();
        }

        private async Task<bool> TableExistsAsync(string tableName, ITransaction? transaction, CancellationToken token)
        {
            await foreach (int _ in Executor.QueryAsync(Dialect.TableExistsQuery(tableName), transaction, "DDL", r => 1, token).ConfigureAwait(false))
                return true;
            return false;
        }

        private List<string> GetColumnNames(string tableName, ITransaction? transaction)
        {
            return Executor.Query(Dialect.ColumnNamesQuery(tableName), transaction, "DDL", r => r.GetValue(0).ToString()!).ToList();
        }

        private async Task<List<string>> GetColumnNamesAsync(string tableName, ITransaction? transaction, CancellationToken token)
        {
            List<string> names = new List<string>();
            await foreach (string name in Executor.QueryAsync(Dialect.ColumnNamesQuery(tableName), transaction, "DDL", r => r.GetValue(0).ToString()!, token).ConfigureAwait(false))
                names.Add(name);
            return names;
        }

        private List<string> GetIndexNames(string tableName, ITransaction? transaction)
        {
            return Executor.Query(Dialect.IndexNamesQuery(tableName), transaction, "DDL", r => r.GetValue(0).ToString()!).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        }

        private async Task<List<string>> GetIndexNamesAsync(string tableName, ITransaction? transaction, CancellationToken token)
        {
            List<string> names = new List<string>();
            await foreach (string name in Executor.QueryAsync(Dialect.IndexNamesQuery(tableName), transaction, "DDL", r => r.GetValue(0).ToString()!, token).ConfigureAwait(false))
            {
                if (!names.Contains(name, StringComparer.OrdinalIgnoreCase)) names.Add(name);
            }

            return names;
        }

        private static List<string> ValidateMapping(EntityMetadata metadata)
        {
            List<string> errors = new List<string>();
            if (metadata.KeyColumns.Count == 0) errors.Add("Entity " + metadata.EntityType.Name + " has no primary key.");
            if (metadata.Columns.Count == 0) errors.Add("Entity " + metadata.EntityType.Name + " has no mapped columns.");
            foreach (IGrouping<string, ColumnMetadata> duplicate in metadata.Columns.GroupBy(c => c.Name, StringComparer.OrdinalIgnoreCase).Where(g => g.Count() > 1))
                errors.Add("Column '" + duplicate.Key + "' is mapped by more than one property.");
            ColumnMetadata? generated = metadata.AutoIncrementColumn;
            if (generated != null)
            {
                Type type = generated.ClrType;
                if (type != typeof(int) && type != typeof(long) && type != typeof(short))
                    errors.Add("AutoIncrement column '" + generated.Name + "' must be an integer type.");
            }

            return errors;
        }

        private static List<string> CompareColumns(EntityMetadata metadata, List<string> tableColumns, List<string> warnings)
        {
            List<string> errors = new List<string>();
            HashSet<string> existing = new HashSet<string>(tableColumns, StringComparer.OrdinalIgnoreCase);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                if (!existing.Contains(column.Name))
                    errors.Add("Column '" + column.Name + "' (property " + column.Property.Name + ") does not exist in table '" + metadata.TableName + "'.");
            }

            HashSet<string> mapped = new HashSet<string>(metadata.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
            foreach (string column in tableColumns)
            {
                if (!mapped.Contains(column))
                    warnings.Add("Column '" + column + "' in table '" + metadata.TableName + "' is not mapped by " + metadata.EntityType.Name + ".");
            }

            return errors;
        }

        private IEnumerable<string> IndexStatements(EntityMetadata metadata, HashSet<string> existing)
        {
            Dictionary<string, List<KeyValuePair<int, ColumnMetadata>>> named = new Dictionary<string, List<KeyValuePair<int, ColumnMetadata>>>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, bool> unique = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string[]?> included = new Dictionary<string, string[]?>(StringComparer.OrdinalIgnoreCase);
            foreach (ColumnMetadata column in metadata.Columns)
            {
                foreach (IndexAttribute index in column.Indexes)
                {
                    string name = index.Name ?? ("idx_" + metadata.TableName + "_" + column.Name);
                    if (!named.TryGetValue(name, out List<KeyValuePair<int, ColumnMetadata>>? columns))
                    {
                        columns = new List<KeyValuePair<int, ColumnMetadata>>();
                        named[name] = columns;
                    }

                    columns.Add(new KeyValuePair<int, ColumnMetadata>(index.Order, column));
                    unique[name] = (unique.TryGetValue(name, out bool u) && u) || index.IsUnique;
                    if (index.IncludedColumns != null) included[name] = index.IncludedColumns;
                }
            }

            foreach (KeyValuePair<string, List<KeyValuePair<int, ColumnMetadata>>> index in named)
            {
                if (existing.Contains(index.Key)) continue;
                List<string> columns = index.Value.OrderBy(c => c.Key).Select(c => c.Value.Name).ToList();
                included.TryGetValue(index.Key, out string[]? include);
                yield return Dialect.CreateIndexSql(index.Key, metadata.TableName, columns, unique[index.Key], include);
            }

            foreach (CompositeIndexAttribute composite in metadata.CompositeIndexes)
            {
                if (existing.Contains(composite.Name)) continue;
                List<string> columns = composite.ColumnNames.Select(name => (metadata.FindColumnByName(name) ?? metadata.FindColumnByProperty(name))?.Name ?? name).ToList();
                yield return Dialect.CreateIndexSql(composite.Name, metadata.TableName, columns, composite.IsUnique, composite.IncludedColumns);
            }
        }

        #endregion
    }
}
