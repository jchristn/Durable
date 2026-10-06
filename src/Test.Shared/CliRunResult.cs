namespace Test.Shared
{
    /// <summary>
    /// The outcome of an in-process durable tool invocation.
    /// </summary>
    public class CliRunResult
    {
        /// <summary>Gets the exit code.</summary>
        public int ExitCode { get; }

        /// <summary>Gets the text written to the output stream.</summary>
        public string Output { get; }

        /// <summary>Gets the text written to the error stream.</summary>
        public string Error { get; }

        /// <summary>
        /// Instantiates a result.
        /// </summary>
        /// <param name="exitCode">Exit code.</param>
        /// <param name="output">Output text.</param>
        /// <param name="error">Error text.</param>
        public CliRunResult(int exitCode, string output, string error)
        {
            ExitCode = exitCode;
            Output = output;
            Error = error;
        }

        /// <summary>
        /// Returns a description used in assertion messages.
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return "exit " + ExitCode + "\n--- output ---\n" + Output + "\n--- error ---\n" + Error;
        }
    }
}
