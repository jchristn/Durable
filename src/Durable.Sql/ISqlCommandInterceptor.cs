namespace Durable.Sql
{
    using System;

    /// <summary>
    /// Observes (and may modify) every command a SQL repository executes. Register through
    /// <see cref="SqlRepositoryOptions.Interceptors"/>. Callbacks run synchronously on the executing flow and should be fast.
    /// Thread safety: implementations must be thread-safe; one instance may observe concurrent commands.
    /// </summary>
    public interface ISqlCommandInterceptor
    {
        /// <summary>
        /// Called before a command executes. The command text and parameters may be modified.
        /// </summary>
        /// <param name="context">Command context. Never null.</param>
        void CommandExecuting(SqlCommandContext context);

        /// <summary>
        /// Called after a command completes successfully (for readers, after the reader is closed).
        /// </summary>
        /// <param name="context">Command context. Never null.</param>
        /// <param name="elapsed">Execution time.</param>
        /// <param name="rowsAffected">Rows affected or read, when known; otherwise null.</param>
        void CommandExecuted(SqlCommandContext context, TimeSpan elapsed, long? rowsAffected);

        /// <summary>
        /// Called when a command throws. The exception is rethrown after all interceptors run.
        /// </summary>
        /// <param name="context">Command context. Never null.</param>
        /// <param name="exception">The exception. Never null.</param>
        /// <param name="elapsed">Time until failure.</param>
        void CommandFailed(SqlCommandContext context, Exception exception, TimeSpan elapsed);
    }
}
