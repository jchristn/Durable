namespace Durable.Tool
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Process entry point of the <c>durable</c> command-line tool. All behavior lives in <see cref="DurableCli"/>.
    /// </summary>
    internal static class Program
    {
        /// <summary>
        /// Runs the tool with the process console streams.
        /// </summary>
        /// <param name="args">Command-line arguments.</param>
        /// <returns>The process exit code (see <see cref="ExitCodes"/>).</returns>
        public static Task<int> Main(string[] args) => DurableCli.RunAsync(args, Console.Out, Console.Error, CancellationToken.None);
    }
}
