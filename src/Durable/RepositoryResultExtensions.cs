namespace Durable
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using System;

    /// <summary>
    /// Extension methods to provide backward compatibility for repository results.
    /// </summary>
    public static class RepositoryResultExtensions
    {
        /// <summary>
        /// Extracts just the data from a DurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The durable result.</param>
        /// <returns>The enumerable data without the query information.</returns>
        public static IEnumerable<T> AsEnumerable<T>(this IDurableResult<T> result)
        {
            return result?.Result ?? Enumerable.Empty<T>();
        }

        /// <summary>
        /// Extracts just the async enumerable data from an AsyncDurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The async durable result.</param>
        /// <returns>The async enumerable data without the query information.</returns>
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IAsyncDurableResult<T> result)
        {
            return result?.Result ?? AsyncEnumerableHelper.Empty<T>();
        }

        /// <summary>
        /// Extension method to get async enumerable data from a Task of AsyncDurableResult.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="resultTask">The task containing the async durable result.</param>
        /// <returns>An async enumerable of the data.</returns>
        public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this Task<IAsyncDurableResult<T>> resultTask)
        {
            IAsyncDurableResult<T> result = await resultTask;
            await foreach (var item in result.AsAsyncEnumerable())
            {
                yield return item;
            }
        }

        /// <summary>
        /// Extracts a single entity from a DurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="result">The durable result.</param>
        /// <returns>The first entity or default.</returns>
        public static T AsEntity<T>(this IDurableResult<T> result)
        {
            if (result?.Result == null) return default(T)!;
            return result.Result.FirstOrDefault() ?? default(T)!;
        }

        /// <summary>
        /// Extracts a single entity from a Task&lt;DurableResult&lt;T&gt;&gt; for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="resultTask">The task containing the durable result.</param>
        /// <returns>The first entity or default.</returns>
        public static async Task<T> AsEntity<T>(this Task<IDurableResult<T>> resultTask)
        {
            IDurableResult<T> result = await resultTask;
            return result.AsEntity();
        }

        /// <summary>
        /// Extracts a single value from a DurableResult for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="result">The durable result.</param>
        /// <returns>The first value or default.</returns>
        public static T AsValue<T>(this IDurableResult<T> result)
        {
            if (result?.Result == null) return default(T)!;
            return result.Result.FirstOrDefault() ?? default(T)!;
        }

        /// <summary>
        /// Extracts a single value from a Task&lt;DurableResult&lt;T&gt;&gt; for backward compatibility.
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="resultTask">The task containing the durable result.</param>
        /// <returns>The first value or default.</returns>
        public static async Task<T> AsValue<T>(this Task<IDurableResult<T>> resultTask)
        {
            IDurableResult<T> result = await resultTask;
            return result.AsValue();
        }

        /// <summary>
        /// Extracts a count value from a DurableResult&lt;int&gt; for backward compatibility.
        /// </summary>
        /// <param name="result">The durable result.</param>
        /// <returns>The count value.</returns>
        public static int AsCount(this IDurableResult<int> result)
        {
            return result?.Result?.FirstOrDefault() ?? 0;
        }

        /// <summary>
        /// Extracts a count value from a Task&lt;DurableResult&lt;int&gt;&gt; for backward compatibility.
        /// </summary>
        /// <param name="resultTask">The task containing the durable result.</param>
        /// <returns>The count value.</returns>
        public static async Task<int> AsCount(this Task<IDurableResult<int>> resultTask)
        {
            IDurableResult<int> result = await resultTask;
            return result.AsCount();
        }

        /// <summary>
        /// Creates an entity and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <returns>A durable result containing the created entity and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static IDurableResult<T> CreateWithQuery<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            EnableSqlCaptureTemporarily(repository, () =>
            {
                T result = repository.Create(entity, transaction!);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<T>(capturedSql ?? string.Empty, new T[] { result });
            }, out IDurableResult<T> durableResult);

            return durableResult;
        }

        /// <summary>
        /// Asynchronously creates an entity and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with a durable result containing the created entity and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static async Task<IDurableResult<T>> CreateWithQueryAsync<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            return await EnableSqlCaptureTemporarilyAsync(repository, async () =>
            {
                T result = await repository.CreateAsync(entity, transaction!, token).ConfigureAwait(false);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<T>(capturedSql ?? string.Empty, new T[] { result });
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads multiple entities and returns both the results and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="predicate">Optional predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <returns>A durable result containing the entities and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public static IDurableResult<T> ReadManyWithQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));

            EnableSqlCaptureTemporarily(repository, () =>
            {
                IEnumerable<T> result = repository.ReadMany(predicate!, transaction!);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<T>(capturedSql ?? string.Empty, result);
            }, out IDurableResult<T> durableResult);

            return durableResult;
        }

        /// <summary>
        /// Asynchronously reads multiple entities and returns both the results and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="predicate">Optional predicate to filter entities.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with a durable result containing the entities and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository is null.</exception>
        public static async Task<IDurableResult<T>> ReadManyWithQueryAsync<T>(this IRepository<T> repository, Expression<Func<T, bool>>? predicate = null, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));

            return await EnableSqlCaptureTemporarilyAsync(repository, async () =>
            {
                List<T> result = new List<T>();
                await foreach (T item in repository.ReadManyAsync(predicate!, transaction!, token).ConfigureAwait(false))
                {
                    token.ThrowIfCancellationRequested();
                    result.Add(item);
                }
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<T>(capturedSql ?? string.Empty, result);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates an entity and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <returns>A durable result containing the updated entity and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static IDurableResult<T> UpdateWithQuery<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            EnableSqlCaptureTemporarily(repository, () =>
            {
                T result = repository.Update(entity, transaction!);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<T>(capturedSql ?? string.Empty, new T[] { result });
            }, out IDurableResult<T> durableResult);

            return durableResult;
        }

        /// <summary>
        /// Asynchronously updates an entity and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with a durable result containing the updated entity and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static async Task<IDurableResult<T>> UpdateWithQueryAsync<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            return await EnableSqlCaptureTemporarilyAsync(repository, async () =>
            {
                T result = await repository.UpdateAsync(entity, transaction!, token).ConfigureAwait(false);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<T>(capturedSql ?? string.Empty, new T[] { result });
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Deletes an entity and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <returns>A durable result containing the deletion result and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static IDurableResult<bool> DeleteWithQuery<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            EnableSqlCaptureTemporarily(repository, () =>
            {
                bool result = repository.Delete(entity, transaction!);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<bool>(capturedSql ?? string.Empty, new bool[] { result });
            }, out IDurableResult<bool> durableResult);

            return durableResult;
        }

        /// <summary>
        /// Asynchronously deletes an entity and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with a durable result containing the deletion result and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static async Task<IDurableResult<bool>> DeleteWithQueryAsync<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            return await EnableSqlCaptureTemporarilyAsync(repository, async () =>
            {
                bool result = await repository.DeleteAsync(entity, transaction!, token).ConfigureAwait(false);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<bool>(capturedSql ?? string.Empty, new bool[] { result });
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Deletes multiple entities that match the specified predicate and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <returns>A durable result containing the number of deleted entities and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or predicate is null.</exception>
        public static IDurableResult<int> DeleteManyWithQuery<T>(this IRepository<T> repository, Expression<Func<T, bool>> predicate, ITransaction? transaction = null) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            EnableSqlCaptureTemporarily(repository, () =>
            {
                int result = repository.DeleteMany(predicate, transaction!);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<int>(capturedSql ?? string.Empty, new int[] { result });
            }, out IDurableResult<int> durableResult);

            return durableResult;
        }

        /// <summary>
        /// Asynchronously deletes multiple entities that match the specified predicate and returns both the result and the executed SQL query.
        /// Enables SQL capture for the duration of this operation.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with a durable result containing the number of deleted entities and the executed SQL query.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or predicate is null.</exception>
        public static async Task<IDurableResult<int>> DeleteManyWithQueryAsync<T>(this IRepository<T> repository, Expression<Func<T, bool>> predicate, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            return await EnableSqlCaptureTemporarilyAsync(repository, async () =>
            {
                int result = await repository.DeleteManyAsync(predicate, transaction!, token).ConfigureAwait(false);
                string? capturedSql = GetCapturedSql(repository);
                return new DurableResult<int>(capturedSql ?? string.Empty, new int[] { result });
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Automatically determines whether to return standard results or SQL-enhanced results based on configuration.
        /// Respects global, thread-local, and instance-level SQL tracking settings.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <returns>Either T or IDurableResult&lt;T&gt; depending on configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static object CreateAuto<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            if (ShouldIncludeQuery(repository))
                return repository.CreateWithQuery(entity, transaction);
            else
                return repository.Create(entity, transaction!);
        }

        /// <summary>
        /// Automatically determines whether to return standard results or SQL-enhanced results based on configuration.
        /// Respects global, thread-local, and instance-level SQL tracking settings.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="repository">The repository instance.</param>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Either Task&lt;T&gt; or Task&lt;IDurableResult&lt;T&gt;&gt; depending on configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or entity is null.</exception>
        public static async Task<object> CreateAutoAsync<T>(this IRepository<T> repository, T entity, ITransaction? transaction = null, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            if (ShouldIncludeQuery(repository))
                return await repository.CreateWithQueryAsync(entity, transaction, token).ConfigureAwait(false);
            else
                return await repository.CreateAsync(entity, transaction!, token).ConfigureAwait(false);
        }

        private static bool ShouldIncludeQuery<T>(IRepository<T> repository) where T : class, new()
        {
            bool? instanceLevelSetting = null;
            if (repository is ISqlTrackingConfiguration config)
            {
                instanceLevelSetting = config.IncludeQueryInResults;
            }

            (bool effectiveSetting, string source) effectiveSettingResult = DurableConfiguration.ResolveIncludeQuerySetting(instanceLevelSetting);
            return effectiveSettingResult.effectiveSetting;
        }

        private static void EnableSqlCaptureTemporarily<T, TResult>(IRepository<T> repository, Func<TResult> operation, out TResult result) where T : class, new()
        {
            if (repository is ISqlCapture sqlCapture)
            {
                using (new SqlCaptureScope(sqlCapture))
                {
                    result = operation();
                }
            }
            else
            {
                result = operation();
            }
        }

        private static async Task<TResult> EnableSqlCaptureTemporarilyAsync<T, TResult>(IRepository<T> repository, Func<Task<TResult>> operation) where T : class, new()
        {
            if (repository is ISqlCapture sqlCapture)
            {
                using (new SqlCaptureScope(sqlCapture))
                {
                    return await operation().ConfigureAwait(false);
                }
            }
            else
            {
                return await operation().ConfigureAwait(false);
            }
        }

        private static string? GetCapturedSql<T>(IRepository<T> repository) where T : class, new()
        {
            return repository is ISqlCapture sqlCapture ? sqlCapture.LastExecutedSql : null;
        }
    }
}