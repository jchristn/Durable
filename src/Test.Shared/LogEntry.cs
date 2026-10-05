namespace Test.Shared
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A single log record captured by <see cref="InMemoryLogger"/>. Immutable and thread safe.
    /// </summary>
    public class LogEntry
    {
        #region Public-Members

        /// <summary>
        /// Gets the level the record was written at.
        /// </summary>
        public LogLevel Level { get; }

        /// <summary>
        /// Gets the formatted message. Never null.
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Gets the exception attached to the record, or null when none was supplied.
        /// </summary>
        public Exception? Exception { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="LogEntry"/> class.
        /// </summary>
        /// <param name="level">The log level.</param>
        /// <param name="message">The formatted message; null is stored as an empty string.</param>
        /// <param name="exception">The attached exception, or null.</param>
        public LogEntry(LogLevel level, string? message, Exception? exception)
        {
            Level = level;
            Message = message ?? string.Empty;
            Exception = exception;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the level and message.
        /// </summary>
        /// <returns>A display string.</returns>
        public override string ToString()
        {
            return Level + ": " + Message;
        }

        #endregion
    }
}
