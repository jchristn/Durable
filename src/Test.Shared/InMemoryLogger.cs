namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A minimal <see cref="ILogger"/> that records every enabled log call in memory for assertions.
    /// Thread safe: entries may be written concurrently and read through <see cref="Snapshot"/>.
    /// </summary>
    public class InMemoryLogger : ILogger
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the minimum level that is recorded. Default: <see cref="LogLevel.Trace"/>.
        /// </summary>
        public LogLevel MinimumLevel { get; set; } = LogLevel.Trace;

        #endregion

        #region Private-Members

        private readonly object _Lock = new object();
        private readonly List<LogEntry> _Entries = new List<LogEntry>();

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a copy of the recorded entries in write order.
        /// </summary>
        /// <returns>A new list; never null.</returns>
        public List<LogEntry> Snapshot()
        {
            lock (_Lock)
            {
                return new List<LogEntry>(_Entries);
            }
        }

        /// <summary>
        /// Removes all recorded entries.
        /// </summary>
        public void Clear()
        {
            lock (_Lock)
            {
                _Entries.Clear();
            }
        }

        /// <summary>
        /// Scopes are not tracked; always returns null.
        /// </summary>
        /// <typeparam name="TState">The scope state type.</typeparam>
        /// <param name="state">The scope state.</param>
        /// <returns>Always null.</returns>
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        /// <summary>
        /// Returns true when <paramref name="logLevel"/> is at or above <see cref="MinimumLevel"/>.
        /// </summary>
        /// <param name="logLevel">The level to check.</param>
        /// <returns>True when enabled.</returns>
        public bool IsEnabled(LogLevel logLevel)
        {
            return logLevel != LogLevel.None && logLevel >= MinimumLevel;
        }

        /// <summary>
        /// Records a log entry with its formatted message.
        /// </summary>
        /// <typeparam name="TState">The state type.</typeparam>
        /// <param name="logLevel">The level.</param>
        /// <param name="eventId">The event id (ignored).</param>
        /// <param name="state">The state.</param>
        /// <param name="exception">The exception, or null.</param>
        /// <param name="formatter">The message formatter.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="formatter"/> is null.</exception>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            if (!IsEnabled(logLevel)) return;
            LogEntry entry = new LogEntry(logLevel, formatter(state, exception), exception);
            lock (_Lock)
            {
                _Entries.Add(entry);
            }
        }

        #endregion
    }
}
