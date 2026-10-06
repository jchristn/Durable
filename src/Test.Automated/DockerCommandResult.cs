namespace Test.Automated
{
    /// <summary>
    /// Captured standard output and standard error of a docker CLI invocation.
    /// </summary>
    internal sealed class DockerCommandResult
    {
        /// <summary>
        /// Instantiates a result.
        /// </summary>
        /// <param name="standardOutput">Standard output. Never null.</param>
        /// <param name="standardError">Standard error. Never null.</param>
        public DockerCommandResult(string standardOutput, string standardError)
        {
            StandardOutput = standardOutput;
            StandardError = standardError;
        }

        /// <summary>
        /// Gets the standard output. Never null.
        /// </summary>
        public string StandardOutput { get; }

        /// <summary>
        /// Gets the standard error. Never null.
        /// </summary>
        public string StandardError { get; }
    }
}
