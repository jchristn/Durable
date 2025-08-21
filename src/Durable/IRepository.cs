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

    // Repository interfaces with transaction support
    public interface IRepository<T> where T : class, new()
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
        // Read operations
        T ReadFirst(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        T ReadFirstOrDefault(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        T ReadSingle(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        T ReadSingleOrDefault(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        IEnumerable<T> ReadMany(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        IEnumerable<T> ReadAll(ITransaction transaction = null);
        T ReadById(object id, ITransaction transaction = null);

        // Read operations - Async variants
        Task<T> ReadFirstAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        Task<T> ReadFirstOrDefaultAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        Task<T> ReadSingleAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        Task<T> ReadSingleOrDefaultAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        IAsyncEnumerable<T> ReadManyAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        IAsyncEnumerable<T> ReadAllAsync(ITransaction transaction = null, CancellationToken token = default);
        Task<T> ReadByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default);

        // Existence checks
        bool Exists(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        bool ExistsById(object id, ITransaction transaction = null);

        // Existence checks - Async variants
        Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        Task<bool> ExistsByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default);

        // Count operations
        int Count(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);

        // Count operations - Async variants
        Task<int> CountAsync(Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);

        // Aggregate operations
        TResult Max<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        TResult Min<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        decimal Average(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);
        decimal Sum(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null);

        // Aggregate operations - Async variants
        Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        Task<decimal> AverageAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);
        Task<decimal> SumAsync(Expression<Func<T, decimal>> selector, Expression<Func<T, bool>> predicate = null, ITransaction transaction = null, CancellationToken token = default);

        // Create operations
        T Create(T entity, ITransaction transaction = null);
        IEnumerable<T> CreateMany(IEnumerable<T> entities, ITransaction transaction = null);

        // Create operations - Async variants
        Task<T> CreateAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        Task<IEnumerable<T>> CreateManyAsync(IEnumerable<T> entities, ITransaction transaction = null, CancellationToken token = default);

        // Update operations
        T Update(T entity, ITransaction transaction = null);
        int UpdateMany(Expression<Func<T, bool>> predicate, Action<T> updateAction, ITransaction transaction = null);
        int UpdateField<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null);

        // Update operations - Async variants
        Task<T> UpdateAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        Task<int> UpdateManyAsync(Expression<Func<T, bool>> predicate, Func<T, Task> updateAction, ITransaction transaction = null, CancellationToken token = default);
        Task<int> UpdateFieldAsync<TField>(Expression<Func<T, bool>> predicate, Expression<Func<T, TField>> field, TField value, ITransaction transaction = null, CancellationToken token = default);

        // Batch operations
        int BatchUpdate(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null);
        int BatchDelete(Expression<Func<T, bool>> predicate, ITransaction transaction = null);

        // Batch operations - Async variants
        Task<int> BatchUpdateAsync(Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updateExpression, ITransaction transaction = null, CancellationToken token = default);
        Task<int> BatchDeleteAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);

        // Delete operations
        bool Delete(T entity, ITransaction transaction = null);
        bool DeleteById(object id, ITransaction transaction = null);
        int DeleteMany(Expression<Func<T, bool>> predicate, ITransaction transaction = null);
        int DeleteAll(ITransaction transaction = null);

        // Delete operations - Async variants
        Task<bool> DeleteAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        Task<bool> DeleteByIdAsync(object id, ITransaction transaction = null, CancellationToken token = default);
        Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, ITransaction transaction = null, CancellationToken token = default);
        Task<int> DeleteAllAsync(ITransaction transaction = null, CancellationToken token = default);

        // Upsert operations
        T Upsert(T entity, ITransaction transaction = null);
        IEnumerable<T> UpsertMany(IEnumerable<T> entities, ITransaction transaction = null);

        // Upsert operations - Async variants
        Task<T> UpsertAsync(T entity, ITransaction transaction = null, CancellationToken token = default);
        Task<IEnumerable<T>> UpsertManyAsync(IEnumerable<T> entities, ITransaction transaction = null, CancellationToken token = default);

        // Raw SQL operations
        IEnumerable<T> FromSql(string sql, ITransaction transaction = null, params object[] parameters);
        IEnumerable<TResult> FromSql<TResult>(string sql, ITransaction transaction = null, params object[] parameters) where TResult : new();
        int ExecuteSql(string sql, ITransaction transaction = null, params object[] parameters);

        // Raw SQL operations - Async variants
        IAsyncEnumerable<T> FromSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters);
        IAsyncEnumerable<TResult> FromSqlAsync<TResult>(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters) where TResult : new();
        Task<int> ExecuteSqlAsync(string sql, ITransaction transaction = null, CancellationToken token = default, params object[] parameters);

        // Advanced query builder
        IQueryBuilder<T> Query(ITransaction transaction = null);

        // Transaction support
        ITransaction BeginTransaction();
        Task<ITransaction> BeginTransactionAsync(CancellationToken token = default);
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
    }
}