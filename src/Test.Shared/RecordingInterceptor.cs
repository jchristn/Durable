namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using Durable.Sql;

    /// <summary>
    /// An <see cref="ISqlCommandInterceptor"/> that records each callback as text of the form
    /// <c>Executing:SELECT</c>, <c>Executed:INSERT</c> or <c>Failed:RAW</c>, and can optionally append a SQL
    /// comment to every command before it runs. Thread safe.
    /// </summary>
    public class RecordingInterceptor : ISqlCommandInterceptor
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets a SQL comment appended to every command text in <see cref="CommandExecuting"/>,
        /// for example <c>/* tag */</c>. Null (the default) leaves command text unchanged.
        /// </summary>
        public string? AppendComment { get; set; } = null;

        /// <summary>
        /// Gets the exception passed to the most recent <see cref="CommandFailed"/> call, or null.
        /// </summary>
        public Exception? LastFailure
        {
            get
            {
                lock (_Lock)
                {
                    return _LastFailure;
                }
            }
        }

        #endregion

        #region Private-Members

        private readonly object _Lock = new object();
        private readonly List<string> _Events = new List<string>();
        private readonly List<string> _ExecutedCommandText = new List<string>();
        private Exception? _LastFailure;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a copy of the recorded events in order.
        /// </summary>
        /// <returns>A new list; never null.</returns>
        public List<string> Events()
        {
            lock (_Lock)
            {
                return new List<string>(_Events);
            }
        }

        /// <summary>
        /// Returns a copy of the command text observed in each <see cref="CommandExecuted"/> callback.
        /// </summary>
        /// <returns>A new list; never null.</returns>
        public List<string> ExecutedCommandText()
        {
            lock (_Lock)
            {
                return new List<string>(_ExecutedCommandText);
            }
        }

        /// <summary>
        /// Records the executing event and applies <see cref="AppendComment"/>.
        /// </summary>
        /// <param name="context">The command context.</param>
        public void CommandExecuting(SqlCommandContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            if (AppendComment != null) context.Command.CommandText = context.Command.CommandText + " " + AppendComment;
            lock (_Lock)
            {
                _Events.Add("Executing:" + context.Operation);
            }
        }

        /// <summary>
        /// Records the executed event and the command text.
        /// </summary>
        /// <param name="context">The command context.</param>
        /// <param name="elapsed">The elapsed time.</param>
        /// <param name="rowsAffected">Rows affected or read, if known.</param>
        public void CommandExecuted(SqlCommandContext context, TimeSpan elapsed, long? rowsAffected)
        {
            ArgumentNullException.ThrowIfNull(context);
            lock (_Lock)
            {
                _Events.Add("Executed:" + context.Operation);
                _ExecutedCommandText.Add(context.Command.CommandText);
            }
        }

        /// <summary>
        /// Records the failed event and the exception.
        /// </summary>
        /// <param name="context">The command context.</param>
        /// <param name="exception">The failure.</param>
        /// <param name="elapsed">The elapsed time.</param>
        public void CommandFailed(SqlCommandContext context, Exception exception, TimeSpan elapsed)
        {
            ArgumentNullException.ThrowIfNull(context);
            lock (_Lock)
            {
                _Events.Add("Failed:" + context.Operation);
                _LastFailure = exception;
            }
        }

        #endregion
    }
}
