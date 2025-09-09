namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public static class TransactionScopeExtensions
    {
        public static void ExecuteInTransactionScope<T>(this IRepository<T> repository, Action action) where T : class, new()
        {
            using TransactionScope scope = TransactionScope.Create(repository);
            action();
            scope.Complete();
        }

        public static TResult ExecuteInTransactionScope<T, TResult>(this IRepository<T> repository, Func<TResult> func) where T : class, new()
        {
            using TransactionScope scope = TransactionScope.Create(repository);
            TResult result = func();
            scope.Complete();
            return result;
        }

        public static async Task ExecuteInTransactionScopeAsync<T>(this IRepository<T> repository, Func<Task> func, CancellationToken token = default) where T : class, new()
        {
            using TransactionScope scope = await TransactionScope.CreateAsync(repository, token);
            await func();
            await scope.CompleteAsync(token);
        }

        public static async Task<TResult> ExecuteInTransactionScopeAsync<T, TResult>(this IRepository<T> repository, Func<Task<TResult>> func, CancellationToken token = default) where T : class, new()
        {
            using TransactionScope scope = await TransactionScope.CreateAsync(repository, token);
            TResult result = await func();
            await scope.CompleteAsync(token);
            return result;
        }

        public static void ExecuteInTransactionScope(this ITransaction transaction, Action action)
        {
            using TransactionScope scope = TransactionScope.Create(transaction);
            action();
            scope.Complete();
        }

        public static TResult ExecuteInTransactionScope<TResult>(this ITransaction transaction, Func<TResult> func)
        {
            using TransactionScope scope = TransactionScope.Create(transaction);
            TResult result = func();
            scope.Complete();
            return result;
        }

        public static async Task ExecuteInTransactionScopeAsync(this ITransaction transaction, Func<Task> func, CancellationToken token = default)
        {
            using TransactionScope scope = TransactionScope.Create(transaction);
            await func();
            await scope.CompleteAsync(token);
        }

        public static async Task<TResult> ExecuteInTransactionScopeAsync<TResult>(this ITransaction transaction, Func<Task<TResult>> func, CancellationToken token = default)
        {
            using TransactionScope scope = TransactionScope.Create(transaction);
            TResult result = await func();
            await scope.CompleteAsync(token);
            return result;
        }

        public static void ExecuteWithSavepoint(this ITransaction transaction, Action action, string? savepointName = null)
        {
            using ISavepoint savepoint = transaction.CreateSavepoint(savepointName);
            try
            {
                action();
                savepoint.Release();
            }
            catch
            {
                savepoint.Rollback();
                throw;
            }
        }

        public static TResult ExecuteWithSavepoint<TResult>(this ITransaction transaction, Func<TResult> func, string? savepointName = null)
        {
            using ISavepoint savepoint = transaction.CreateSavepoint(savepointName);
            try
            {
                TResult result = func();
                savepoint.Release();
                return result;
            }
            catch
            {
                savepoint.Rollback();
                throw;
            }
        }

        public static async Task ExecuteWithSavepointAsync(this ITransaction transaction, Func<Task> func, string? savepointName = null, CancellationToken token = default)
        {
            using ISavepoint savepoint = await transaction.CreateSavepointAsync(savepointName, token);
            try
            {
                await func();
                await savepoint.ReleaseAsync(token);
            }
            catch
            {
                await savepoint.RollbackAsync(token);
                throw;
            }
        }

        public static async Task<TResult> ExecuteWithSavepointAsync<TResult>(this ITransaction transaction, Func<Task<TResult>> func, string? savepointName = null, CancellationToken token = default)
        {
            using ISavepoint savepoint = await transaction.CreateSavepointAsync(savepointName, token);
            try
            {
                TResult result = await func();
                await savepoint.ReleaseAsync(token);
                return result;
            }
            catch
            {
                await savepoint.RollbackAsync(token);
                throw;
            }
        }
        
    }
}