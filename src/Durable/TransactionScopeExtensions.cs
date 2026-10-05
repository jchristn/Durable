namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extension methods for executing operations within transaction scopes.
    /// </summary>
    public static class TransactionScopeExtensions
    {
        /// <summary>
        /// Executes an action within a transaction scope for the specified repository.
        /// </summary>
        /// <typeparam name="T">The type of entity managed by the repository.</typeparam>
        /// <param name="repository">The repository to create a transaction scope for.</param>
        /// <param name="action">The action to execute within the transaction scope.</param>
        /// <exception cref="ArgumentNullException">Thrown when repository or action is null.</exception>
        public static void ExecuteInTransactionScope<T>(this IRepository<T> repository, Action action) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (action == null) throw new ArgumentNullException(nameof(action));

            using TransactionScope scope = TransactionScope.Create(repository);
            try
            {
                action();
                scope.Complete();
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }

        /// <summary>
        /// Executes a function within a transaction scope for the specified repository and returns the result.
        /// </summary>
        /// <typeparam name="T">The type of entity managed by the repository.</typeparam>
        /// <typeparam name="TResult">The type of the result returned by the function.</typeparam>
        /// <param name="repository">The repository to create a transaction scope for.</param>
        /// <param name="func">The function to execute within the transaction scope.</param>
        /// <returns>The result of the function execution.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or func is null.</exception>
        public static TResult ExecuteInTransactionScope<T, TResult>(this IRepository<T> repository, Func<TResult> func) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (func == null) throw new ArgumentNullException(nameof(func));

            using TransactionScope scope = TransactionScope.Create(repository);
            try
            {
                TResult result = func();
                scope.Complete();
                return result;
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }

        /// <summary>
        /// Asynchronously executes a task within a transaction scope for the specified repository.
        /// </summary>
        /// <typeparam name="T">The type of entity managed by the repository.</typeparam>
        /// <param name="repository">The repository to create a transaction scope for.</param>
        /// <param name="func">The task function to execute within the transaction scope.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or func is null.</exception>
        public static async Task ExecuteInTransactionScopeAsync<T>(this IRepository<T> repository, Func<Task> func, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (func == null) throw new ArgumentNullException(nameof(func));

            using TransactionScope scope = await TransactionScope.CreateAsync(repository, token).ConfigureAwait(false);
            try
            {
                await func().ConfigureAwait(false);
                await scope.CompleteAsync(token).ConfigureAwait(false);
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }

        /// <summary>
        /// Asynchronously executes a task function within a transaction scope for the specified repository and returns the result.
        /// </summary>
        /// <typeparam name="T">The type of entity managed by the repository.</typeparam>
        /// <typeparam name="TResult">The type of the result returned by the function.</typeparam>
        /// <param name="repository">The repository to create a transaction scope for.</param>
        /// <param name="func">The task function to execute within the transaction scope.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when repository or func is null.</exception>
        public static async Task<TResult> ExecuteInTransactionScopeAsync<T, TResult>(this IRepository<T> repository, Func<Task<TResult>> func, CancellationToken token = default) where T : class, new()
        {
            if (repository == null) throw new ArgumentNullException(nameof(repository));
            if (func == null) throw new ArgumentNullException(nameof(func));

            using TransactionScope scope = await TransactionScope.CreateAsync(repository, token).ConfigureAwait(false);
            TResult result = await func().ConfigureAwait(false);
            await scope.CompleteAsync(token).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Executes an action within a transaction scope for the specified transaction.
        /// </summary>
        /// <param name="transaction">The transaction to create a transaction scope for.</param>
        /// <param name="action">The action to execute within the transaction scope.</param>
        /// <exception cref="ArgumentNullException">Thrown when transaction or action is null.</exception>
        public static void ExecuteInTransactionScope(this ITransaction transaction, Action action)
        {
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (action == null) throw new ArgumentNullException(nameof(action));

            using TransactionScope scope = TransactionScope.Create(transaction);
            try
            {
                action();
                scope.Complete();
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }

        /// <summary>
        /// Executes a function within a transaction scope for the specified transaction and returns the result.
        /// </summary>
        /// <typeparam name="TResult">The type of the result returned by the function.</typeparam>
        /// <param name="transaction">The transaction to create a transaction scope for.</param>
        /// <param name="func">The function to execute within the transaction scope.</param>
        /// <returns>The result of the function execution.</returns>
        /// <exception cref="ArgumentNullException">Thrown when transaction or func is null.</exception>
        public static TResult ExecuteInTransactionScope<TResult>(this ITransaction transaction, Func<TResult> func)
        {
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (func == null) throw new ArgumentNullException(nameof(func));

            using TransactionScope scope = TransactionScope.Create(transaction);
            try
            {
                TResult result = func();
                scope.Complete();
                return result;
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }

        /// <summary>
        /// Asynchronously executes a task within a transaction scope for the specified transaction.
        /// </summary>
        /// <param name="transaction">The transaction to create a transaction scope for.</param>
        /// <param name="func">The task function to execute within the transaction scope.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when transaction or func is null.</exception>
        public static async Task ExecuteInTransactionScopeAsync(this ITransaction transaction, Func<Task> func, CancellationToken token = default)
        {
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (func == null) throw new ArgumentNullException(nameof(func));

            using TransactionScope scope = TransactionScope.Create(transaction);
            try
            {
                await func().ConfigureAwait(false);
                await scope.CompleteAsync(token).ConfigureAwait(false);
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }

        /// <summary>
        /// Asynchronously executes a task function within a transaction scope for the specified transaction and returns the result.
        /// </summary>
        /// <typeparam name="TResult">The type of the result returned by the function.</typeparam>
        /// <param name="transaction">The transaction to create a transaction scope for.</param>
        /// <param name="func">The task function to execute within the transaction scope.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation containing the result.</returns>
        /// <exception cref="ArgumentNullException">Thrown when transaction or func is null.</exception>
        public static async Task<TResult> ExecuteInTransactionScopeAsync<TResult>(this ITransaction transaction, Func<Task<TResult>> func, CancellationToken token = default)
        {
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (func == null) throw new ArgumentNullException(nameof(func));

            using TransactionScope scope = TransactionScope.Create(transaction);
            try
            {
                TResult result = await func().ConfigureAwait(false);
                await scope.CompleteAsync(token).ConfigureAwait(false);
                return result;
            }
            catch
            {
                // Don't complete the scope on exception - let it dispose and rollback
                throw;
            }
        }
    }
}
