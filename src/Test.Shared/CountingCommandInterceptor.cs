namespace Test.Shared
{
    using System;
    using System.Collections.Concurrent;
    using Durable.Sql;

    /// <summary>
    /// Command interceptor that counts executed commands per operation name (for example "INCLUDE" or "INSERT").
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public class CountingCommandInterceptor : ISqlCommandInterceptor
    {
        #region Private-Members

        private readonly ConcurrentDictionary<string, int> _Counts = new ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns how many commands with the given operation name have executed successfully.
        /// </summary>
        /// <param name="operation">Operation name. Must not be null.</param>
        /// <returns>The count; zero when none.</returns>
        /// <exception cref="ArgumentNullException">Thrown when operation is null.</exception>
        public int CountOf(string operation)
        {
            ArgumentNullException.ThrowIfNull(operation);
            return _Counts.TryGetValue(operation, out int count) ? count : 0;
        }

        /// <summary>
        /// Resets all counters.
        /// </summary>
        public void Reset()
        {
            _Counts.Clear();
        }

        /// <summary>
        /// Called before a command executes. No-op.
        /// </summary>
        /// <param name="context">Command context.</param>
        public void CommandExecuting(SqlCommandContext context)
        {
        }

        /// <summary>
        /// Called after a command executes; increments the operation counter.
        /// </summary>
        /// <param name="context">Command context. Must not be null.</param>
        /// <param name="elapsed">Elapsed time.</param>
        /// <param name="rowsAffected">Rows affected, when known.</param>
        /// <exception cref="ArgumentNullException">Thrown when context is null.</exception>
        public void CommandExecuted(SqlCommandContext context, TimeSpan elapsed, long? rowsAffected)
        {
            ArgumentNullException.ThrowIfNull(context);
            _Counts.AddOrUpdate(context.Operation, 1, (key, value) => value + 1);
        }

        /// <summary>
        /// Called when a command fails. No-op.
        /// </summary>
        /// <param name="context">Command context.</param>
        /// <param name="exception">Exception.</param>
        /// <param name="elapsed">Elapsed time.</param>
        public void CommandFailed(SqlCommandContext context, Exception exception, TimeSpan elapsed)
        {
        }

        #endregion
    }
}
