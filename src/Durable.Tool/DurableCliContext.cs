namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    /// The environment a <see cref="DurableCli"/> invocation runs in: output streams, working directory and environment
    /// variables. Supplying a context makes the tool fully testable in-process. Instances are not thread-safe; use one per
    /// invocation.
    /// </summary>
    public class DurableCliContext
    {
        /// <summary>
        /// Gets the stream receiving normal command output (results, scripts, help). Never null.
        /// </summary>
        public TextWriter Output { get; }

        /// <summary>
        /// Gets the stream receiving errors and warnings. Never null.
        /// </summary>
        public TextWriter Error { get; }

        /// <summary>
        /// Gets the absolute directory relative paths are resolved against and where <c>durable.json</c> and the default
        /// project are looked up. Never null.
        /// </summary>
        public string WorkingDirectory { get; }

        /// <summary>
        /// Gets the function returning an environment variable's value, or null when it is not set. Never null.
        /// </summary>
        public Func<string, string?> GetEnvironmentVariable { get; }

        /// <summary>
        /// Instantiates a context using the process working directory and environment variables.
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        /// <param name="error">Error stream. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a stream is null.</exception>
        public DurableCliContext(TextWriter output, TextWriter error)
            : this(output, error, Environment.CurrentDirectory, Environment.GetEnvironmentVariable)
        {
        }

        /// <summary>
        /// Instantiates a context with an explicit working directory and environment variables.
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        /// <param name="error">Error stream. Must not be null.</param>
        /// <param name="workingDirectory">Working directory; made absolute. Must not be null or empty.</param>
        /// <param name="environment">Environment variables visible to the tool; null means none.</param>
        /// <exception cref="ArgumentNullException">Thrown when a stream is null.</exception>
        /// <exception cref="ArgumentException">Thrown when workingDirectory is null or empty.</exception>
        public DurableCliContext(TextWriter output, TextWriter error, string workingDirectory, IReadOnlyDictionary<string, string>? environment)
            : this(output, error, workingDirectory, name => environment != null && environment.TryGetValue(name, out string? value) ? value : null)
        {
        }

        /// <summary>
        /// Instantiates a context with an explicit working directory and environment variable lookup.
        /// </summary>
        /// <param name="output">Output stream. Must not be null.</param>
        /// <param name="error">Error stream. Must not be null.</param>
        /// <param name="workingDirectory">Working directory; made absolute. Must not be null or empty.</param>
        /// <param name="getEnvironmentVariable">Environment variable lookup. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when a stream or the lookup is null.</exception>
        /// <exception cref="ArgumentException">Thrown when workingDirectory is null or empty.</exception>
        public DurableCliContext(TextWriter output, TextWriter error, string workingDirectory, Func<string, string?> getEnvironmentVariable)
        {
            Output = output ?? throw new ArgumentNullException(nameof(output));
            Error = error ?? throw new ArgumentNullException(nameof(error));
            if (string.IsNullOrEmpty(workingDirectory)) throw new ArgumentException("Working directory cannot be null or empty.", nameof(workingDirectory));
            WorkingDirectory = Path.GetFullPath(workingDirectory);
            GetEnvironmentVariable = getEnvironmentVariable ?? throw new ArgumentNullException(nameof(getEnvironmentVariable));
        }

        /// <summary>
        /// Resolves a path against <see cref="WorkingDirectory"/>.
        /// </summary>
        /// <param name="path">Absolute or relative path. Must not be null.</param>
        /// <returns>The absolute path.</returns>
        /// <exception cref="ArgumentNullException">Thrown when path is null.</exception>
        public string ResolvePath(string path)
        {
            ArgumentNullException.ThrowIfNull(path);
            return Path.GetFullPath(Path.Combine(WorkingDirectory, path));
        }
    }
}
