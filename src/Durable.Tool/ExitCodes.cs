namespace Durable.Tool
{
    /// <summary>
    /// Process exit codes returned by <see cref="DurableCli"/>.
    /// </summary>
    public static class ExitCodes
    {
        /// <summary>
        /// The command completed successfully.
        /// </summary>
        public const int Success = 0;

        /// <summary>
        /// The command could not run or failed because of the arguments, the configuration, the user's project or the
        /// database (for example an unknown option, a missing connection string, a build failure or a failed migration).
        /// A message explaining the problem is written to the error stream.
        /// </summary>
        public const int CommandError = 1;

        /// <summary>
        /// The tool failed unexpectedly (a defect). The exception, including its stack trace, is written to the error stream.
        /// </summary>
        public const int UnexpectedFailure = 2;
    }
}
