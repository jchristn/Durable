namespace Test.Benchmark
{
    using BenchmarkDotNet.Running;

    /// <summary>
    /// Entry point; forwards command-line arguments (for example <c>--filter '*'</c>) to BenchmarkDotNet.
    /// </summary>
    public static class Program
    {
        /// <summary>
        /// Runs the benchmarks selected on the command line.
        /// </summary>
        /// <param name="args">BenchmarkDotNet arguments.</param>
        public static void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
