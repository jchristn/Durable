namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Repository interface with transaction support for CRUD operations on entities of type T.
    /// </summary>
    /// <typeparam name="T">The entity type that implements class constraint and has a parameterless constructor.</typeparam>
    public interface IRepository<T> where T : class, new()
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
        /// <summary>
        /// Reads the first entity that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The first entity that matches the predicate.</returns>
        T? ReadFirst(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        /// <summary>
        /// Reads the first entity that matches the specified predicate, or returns default if none found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The first entity that matches the predicate, or default if none found.</returns>
        T? ReadFirstOrDefault(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        /// <summary>
        /// Reads a single entity that matches the specified predicate. Throws an exception if zero or more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The single entity that matches the predicate.</returns>
        T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        /// <summary>
        /// Reads a single entity that matches the specified predicate, or returns default if none found. Throws an exception if more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The single entity that matches the predicate, or default if none found.</returns>
        T? ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        /// <summary>
        /// Reads multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>A collection of entities that match the predicate.</returns>
        IEnumerable<T> ReadMany(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        /// <summary>
        /// Reads all entities from the repository.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>A collection of all entities.</returns>
        IEnumerable<T> ReadAll(ITransaction transaction = null);
        /// <summary>
        /// Reads an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to read.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The entity with the specified identifier.</returns>
        T? ReadById(object id, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously reads the first entity that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the first entity that matches the predicate.</returns>
        Task<T?> ReadFirstAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously reads the first entity that matches the specified predicate, or returns default if none found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns the first entity.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the first entity that matches the predicate, or default if none found.</returns>
        Task<T?> ReadFirstOrDefaultAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously reads a single entity that matches the specified predicate. Throws an exception if zero or more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the single entity that matches the predicate.</returns>
        Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously reads a single entity that matches the specified predicate, or returns default if none found. Throws an exception if more than one entity is found.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the single entity that matches the predicate, or default if none found.</returns>
        Task<T?> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously reads multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, returns all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>An async enumerable of entities that match the predicate.</returns>
        IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously reads all entities from the repository.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>An async enumerable of all entities.</returns>
        IAsyncEnumerable<T> ReadAllAsync(ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously reads an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to read.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the entity that has the specified identifier.</returns>
        Task<T?> ReadByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Checks if any entity exists that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>True if any entity matches the predicate; otherwise, false.</returns>
        bool Exists(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        /// <summary>
        /// Checks if an entity exists with the specified identifier.
        /// </summary>
        /// <param name="id">The identifier to check for.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>True if an entity with the specified identifier exists; otherwise, false.</returns>
        bool ExistsById(object id, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously checks if any entity exists that matches the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a result of true if any entity matches the predicate; otherwise, false.</returns>
        Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously checks if an entity exists with the specified identifier.
        /// </summary>
        /// <param name="id">The identifier to check for.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a result of true if an entity with the specified identifier exists; otherwise, false.</returns>
        Task<bool> ExistsByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities that match the predicate.</returns>
        int Count(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously counts the number of entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities. If null, counts all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities that match the predicate.</returns>
        Task<int> CountAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Finds the maximum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the maximum for.</typeparam>
        /// <param name="selector">The property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The maximum value of the specified property.</returns>
        TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        /// <summary>
        /// Finds the minimum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the minimum for.</typeparam>
        /// <param name="selector">The property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The minimum value of the specified property.</returns>
        TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        /// <summary>
        /// Calculates the average value of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">The decimal property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The average value of the specified property.</returns>
        decimal Average(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        /// <summary>
        /// Calculates the sum of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">The decimal property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The sum of the specified property.</returns>
        decimal Sum(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously finds the maximum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the maximum for.</typeparam>
        /// <param name="selector">The property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the maximum value of the specified property.</returns>
        Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously finds the minimum value of the specified property among entities that match the predicate.
        /// </summary>
        /// <typeparam name="TResult">The type of the property to find the minimum for.</typeparam>
        /// <param name="selector">The property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the minimum value of the specified property.</returns>
        Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously calculates the average value of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">The decimal property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the average value of the specified property.</returns>
        Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously calculates the sum of the specified decimal property among entities that match the predicate.
        /// </summary>
        /// <param name="selector">The decimal property selector expression.</param>
        /// <param name="predicate">The predicate to filter entities. If null, considers all entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the sum of the specified property.</returns>
        Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Creates a new entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The created entity.</returns>
        T Create(T entity, ITransaction transaction = null);
        /// <summary>
        /// Creates multiple entities in the repository.
        /// </summary>
        /// <param name="entities">The entities to create.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The created entities.</returns>
        IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously creates a new entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to create.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the created entity.</returns>
        Task<T> CreateAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously creates multiple entities in the repository.
        /// </summary>
        /// <param name="entities">The entities to create.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the created entities.</returns>
        Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Updates an existing entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The updated entity.</returns>
        T Update(T entity, ITransaction transaction = null);
        /// <summary>
        /// Updates multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateAction">The action to perform on each matching entity.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities updated.</returns>
        int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction transaction = null);
        /// <summary>
        /// Updates a specific field on entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field to update.</typeparam>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="field">The field selector expression.</param>
        /// <param name="value">The new value for the field.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities updated.</returns>
        int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously updates an existing entity in the repository.
        /// </summary>
        /// <param name="entity">The entity to update.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the updated entity.</returns>
        Task<T> UpdateAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously updates multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateAction">The async action to perform on each matching entity.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities updated.</returns>
        Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously updates a specific field on entities that match the specified predicate.
        /// </summary>
        /// <typeparam name="TField">The type of the field to update.</typeparam>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="field">The field selector expression.</param>
        /// <param name="value">The new value for the field.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities updated.</returns>
        Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Performs a batch update on entities that match the specified predicate using an update expression.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateExpression">The expression defining how to update the entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities updated.</returns>
        int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null);
        /// <summary>
        /// Performs a batch delete on entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities deleted.</returns>
        int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously performs a batch update on entities that match the specified predicate using an update expression.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to update.</param>
        /// <param name="updateExpression">The expression defining how to update the entities.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities updated.</returns>
        Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously performs a batch delete on entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities deleted.</returns>
        Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Deletes an entity from the repository.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>True if the entity was deleted; otherwise, false.</returns>
        bool Delete(T entity, ITransaction transaction = null);
        /// <summary>
        /// Deletes an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>True if the entity was deleted; otherwise, false.</returns>
        bool DeleteById(object id, ITransaction transaction = null);
        /// <summary>
        /// Deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities deleted.</returns>
        int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        /// <summary>
        /// Deletes all entities from the repository.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The number of entities deleted.</returns>
        int DeleteAll(ITransaction transaction = null);

        /// <summary>
        /// Asynchronously deletes an entity from the repository.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a result of true if the entity was deleted; otherwise, false.</returns>
        Task<bool> DeleteAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously deletes an entity by its identifier.
        /// </summary>
        /// <param name="id">The identifier of the entity to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a result of true if the entity was deleted; otherwise, false.</returns>
        Task<bool> DeleteByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously deletes multiple entities that match the specified predicate.
        /// </summary>
        /// <param name="predicate">The predicate to filter entities to delete.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities deleted.</returns>
        Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously deletes all entities from the repository.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the number of entities deleted.</returns>
        Task<int> DeleteAllAsync(ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Upserts an entity (inserts if new, updates if exists) in the repository.
        /// </summary>
        /// <param name="entity">The entity to upsert.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The upserted entity.</returns>
        T Upsert(T entity, ITransaction transaction = null);
        /// <summary>
        /// Upserts multiple entities (inserts if new, updates if exists) in the repository.
        /// </summary>
        /// <param name="entities">The entities to upsert.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>The upserted entities.</returns>
        IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously upserts an entity (inserts if new, updates if exists) in the repository.
        /// </summary>
        /// <param name="entity">The entity to upsert.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the upserted entity.</returns>
        Task<T> UpsertAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        /// <summary>
        /// Asynchronously upserts multiple entities (inserts if new, updates if exists) in the repository.
        /// </summary>
        /// <param name="entities">The entities to upsert.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with the upserted entities.</returns>
        Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction transaction = null, CancellationToken token = default);

        /// <summary>
        /// Executes a raw SQL query and maps the results to entities of type T.
        /// </summary>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="parameters">The parameters for the SQL query.</param>
        /// <returns>A collection of entities mapped from the SQL query results.</returns>
        IEnumerable<T> FromSql(string sql, ITransaction transaction = null, params object[] parameters);
        /// <summary>
        /// Executes a raw SQL query and maps the results to entities of type TResult.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to.</typeparam>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="parameters">The parameters for the SQL query.</param>
        /// <returns>A collection of entities of type TResult mapped from the SQL query results.</returns>
        IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction transaction = null, params object[] parameters) where TResult : new();
        /// <summary>
        /// Executes a raw SQL command that does not return results (INSERT, UPDATE, DELETE).
        /// </summary>
        /// <param name="sql">The SQL command to execute.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="parameters">The parameters for the SQL command.</param>
        /// <returns>The number of rows affected by the SQL command.</returns>
        int ExecuteSql(string sql, ITransaction transaction = null, params object[] parameters);

        /// <summary>
        /// Asynchronously executes a raw SQL query and maps the results to entities of type T.
        /// </summary>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <param name="parameters">The parameters for the SQL query.</param>
        /// <returns>An async enumerable of entities mapped from the SQL query results.</returns>
        IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters);
        /// <summary>
        /// Asynchronously executes a raw SQL query and maps the results to entities of type TResult.
        /// </summary>
        /// <typeparam name="TResult">The type to map the query results to.</typeparam>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <param name="parameters">The parameters for the SQL query.</param>
        /// <returns>An async enumerable of entities of type TResult mapped from the SQL query results.</returns>
        IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters) where TResult : new();
        /// <summary>
        /// Asynchronously executes a raw SQL command that does not return results (INSERT, UPDATE, DELETE).
        /// </summary>
        /// <param name="sql">The SQL command to execute.</param>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <param name="parameters">The parameters for the SQL command.</param>
        /// <returns>A task representing the asynchronous operation with the number of rows affected by the SQL command.</returns>
        Task<int> ExecuteSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters);

        /// <summary>
        /// Creates an advanced query builder for constructing complex queries.
        /// </summary>
        /// <param name="transaction">The transaction to use for the operation.</param>
        /// <returns>A query builder instance for the entity type.</returns>
        IQueryBuilder<T> Query(ITransaction transaction = null);

        /// <summary>
        /// Begins a new database transaction.
        /// </summary>
        /// <returns>A new transaction instance.</returns>
        ITransaction BeginTransaction();
        /// <summary>
        /// Asynchronously begins a new database transaction.
        /// </summary>
        /// <param name="token">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation with a new transaction instance.</returns>
        Task<ITransaction> BeginTransactionAsync(CancellationToken token = default);

        #region Initialization

        /// <summary>
        /// Creates or validates the database table for the specified entity type.
        /// If the table does not exist, it will be created based on the entity's attributes.
        /// If the table exists, it will be validated against the entity definition.
        /// </summary>
        /// <param name="entityType">The entity type for which to initialize the table.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type is invalid or table validation fails.</exception>
        void InitializeTable(Type entityType, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously creates or validates the database table for the specified entity type.
        /// If the table does not exist, it will be created based on the entity's attributes.
        /// If the table exists, it will be validated against the entity definition.
        /// </summary>
        /// <param name="entityType">The entity type for which to initialize the table.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the entity type is invalid or table validation fails.</exception>
        Task InitializeTableAsync(Type entityType, ITransaction transaction = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates or validates database tables for multiple entity types.
        /// If a table does not exist, it will be created based on the entity's attributes.
        /// If a table exists, it will be validated against the entity definition.
        /// </summary>
        /// <param name="entityTypes">The entity types for which to initialize tables.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when an entity type is invalid or table validation fails.</exception>
        void InitializeTables(IEnumerable<Type> entityTypes, ITransaction transaction = null);

        /// <summary>
        /// Asynchronously creates or validates database tables for multiple entity types.
        /// If a table does not exist, it will be created based on the entity's attributes.
        /// If a table exists, it will be validated against the entity definition.
        /// </summary>
        /// <param name="entityTypes">The entity types for which to initialize tables.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when an entity type is invalid or table validation fails.</exception>
        Task InitializeTablesAsync(IEnumerable<Type> entityTypes, ITransaction transaction = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Validates an entity type's definition without creating or modifying the database table.
        /// Checks for required attributes, valid property configurations, and schema consistency if the table exists.
        /// </summary>
        /// <param name="entityType">The entity type to validate.</param>
        /// <param name="errors">List of validation errors that would prevent table initialization.</param>
        /// <param name="warnings">List of validation warnings about potential issues.</param>
        /// <returns>True if the entity type is valid and can be initialized; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        bool ValidateTable(Type entityType, out List<string> errors, out List<string> warnings);

        /// <summary>
        /// Validates multiple entity types' definitions without creating or modifying database tables.
        /// Checks for required attributes, valid property configurations, and schema consistency if tables exist.
        /// </summary>
        /// <param name="entityTypes">The entity types to validate.</param>
        /// <param name="errors">List of validation errors that would prevent table initialization.</param>
        /// <param name="warnings">List of validation warnings about potential issues.</param>
        /// <returns>True if all entity types are valid and can be initialized; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        bool ValidateTables(IEnumerable<Type> entityTypes, out List<string> errors, out List<string> warnings);

        /// <summary>
        /// Creates indexes for the specified entity type based on Index and CompositeIndex attributes.
        /// This method is automatically called by InitializeTable when createIndexes parameter is true.
        /// Indexes that already exist are skipped (idempotent operation).
        /// </summary>
        /// <param name="entityType">The entity type to create indexes for.</param>
        /// <param name="transaction">Optional transaction to use for index creation.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        void CreateIndexes(Type entityType, ITransaction? transaction = null);

        /// <summary>
        /// Asynchronously creates indexes for the specified entity type based on Index and CompositeIndex attributes.
        /// This method is automatically called by InitializeTableAsync when createIndexes parameter is true.
        /// Indexes that already exist are skipped (idempotent operation).
        /// </summary>
        /// <param name="entityType">The entity type to create indexes for.</param>
        /// <param name="transaction">Optional transaction to use for index creation.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        Task CreateIndexesAsync(Type entityType, ITransaction? transaction = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Drops an index from the database.
        /// </summary>
        /// <param name="indexName">The name of the index to drop.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <exception cref="ArgumentNullException">Thrown when indexName is null or empty.</exception>
        void DropIndex(string indexName, ITransaction? transaction = null);

        /// <summary>
        /// Asynchronously drops an index from the database.
        /// </summary>
        /// <param name="indexName">The name of the index to drop.</param>
        /// <param name="transaction">Optional transaction to use for the operation.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when indexName is null or empty.</exception>
        Task DropIndexAsync(string indexName, ITransaction? transaction = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a list of index names for the specified entity type.
        /// </summary>
        /// <param name="entityType">The entity type to get indexes for.</param>
        /// <returns>A list of index names that exist in the database for this entity's table.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        List<string> GetIndexes(Type entityType);

        /// <summary>
        /// Asynchronously gets a list of index names for the specified entity type.
        /// </summary>
        /// <param name="entityType">The entity type to get indexes for.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation that returns a list of index names.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        Task<List<string>> GetIndexesAsync(Type entityType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates the database if it does not exist.
        /// For file-based databases (SQLite), this ensures the file is created.
        /// For server-based databases, this executes CREATE DATABASE if needed.
        /// </summary>
        void CreateDatabaseIfNotExists();

        /// <summary>
        /// Asynchronously creates the database if it does not exist.
        /// For file-based databases (SQLite), this ensures the file is created.
        /// For server-based databases, this executes CREATE DATABASE if needed.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task CreateDatabaseIfNotExistsAsync(CancellationToken cancellationToken = default);

        #endregion

        /// <summary>
        /// Gets the repository settings used to configure the connection
        /// </summary>
        RepositorySettings Settings { get; }
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
    }
}