namespace Durable.Tool
{
    /// <summary>
    /// The outcome of a child process run by <see cref="ProcessRunner"/>.
    /// </summary>
    internal sealed class ProcessResult
    {
        /// <summary>Gets the exit code.</summary>
        public int ExitCode { get; }

        /// <summary>Gets the captured standard output.</summary>
        public string StandardOutput { get; }

        /// <summary>Gets the captured standard error.</summary>
        public string StandardError { get; }

        /// <summary>Gets standard output followed by standard error.</summary>
        public string CombinedOutput => (StandardOutput + System.Environment.NewLine + StandardError).Trim();

        /// <summary>
        /// Instantiates a result.
        /// </summary>
        /// <param name="exitCode">Exit code.</param>
        /// <param name="standardOutput">Standard output. Must not be null.</param>
        /// <param name="standardError">Standard error. Must not be null.</param>
        public ProcessResult(int exitCode, string standardOutput, string standardError)
        {
            ExitCode = exitCode;
            StandardOutput = standardOutput;
            StandardError = standardError;
        }
    }
}
