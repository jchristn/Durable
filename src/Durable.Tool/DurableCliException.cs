namespace Durable.Tool
{
    using System;

    /// <summary>
    /// A user-facing command error: invalid arguments or configuration, a project that cannot be built or loaded, or a
    /// command that cannot proceed. <see cref="DurableCli"/> reports the message and exits with
    /// <see cref="ExitCodes.CommandError"/>.
    /// </summary>
    public class DurableCliException : Exception
    {
        /// <summary>
        /// Gets the detail written after the message when the error is reported (for example build output); may be null.
        /// </summary>
        public string? Detail { get; }

        /// <summary>
        /// Gets whether the command's help should be suggested after the message.
        /// </summary>
        public bool SuggestHelp { get; }

        /// <summary>
        /// Instantiates the exception.
        /// </summary>
        /// <param name="message">Message explaining the problem and how to fix it. Must not be null.</param>
        public DurableCliException(string message) : base(message)
        {
        }

        /// <summary>
        /// Instantiates the exception.
        /// </summary>
        /// <param name="message">Message explaining the problem and how to fix it. Must not be null.</param>
        /// <param name="detail">Additional detail written after the message; may be null.</param>
        /// <param name="suggestHelp">Whether the command's help should be suggested.</param>
        /// <param name="innerException">The underlying exception; may be null.</param>
        public DurableCliException(string message, string? detail, bool suggestHelp = false, Exception? innerException = null) : base(message, innerException)
        {
            Detail = detail;
            SuggestHelp = suggestHelp;
        }
    }
}
