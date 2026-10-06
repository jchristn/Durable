namespace Test.Aot
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Native AOT end-to-end check for Durable. Publish with
    /// <c>dotnet publish src/Test.Aot -c Release -r &lt;rid&gt; -f &lt;tfm&gt;</c> and run the produced binary; it prints PASS/FAIL per
    /// check and exits non-zero on any failure. Running it on the JIT (<c>dotnet run</c>) executes the same checks, which
    /// gives the JIT timing to compare with.
    /// Arguments: <c>--rows N</c> (default 10000), <c>--iterations N</c> (default 15).
    /// </summary>
    public static class Program
    {
        public static async Task<int> Main(string[] args)
        {
            int rows = 10000;
            int iterations = 15;
            for (int i = 0; i + 1 < args.Length; i++)
            {
                if (args[i] == "--rows") rows = int.Parse(args[i + 1]);
                if (args[i] == "--iterations") iterations = int.Parse(args[i + 1]);
            }

            Console.WriteLine("Durable AOT check: " + RuntimeMode.Describe()
                + ", " + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);

            CheckRunner runner = new CheckRunner();
            try
            {
                await SqliteScenario.RunAsync(runner, rows, iterations).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                runner.Fail("[sqlite] scenario aborted", ex);
            }

            try
            {
                await InMemoryScenario.RunAsync(runner).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                runner.Fail("[inmemory] scenario aborted", ex);
            }

#if NET9_0_OR_GREATER
            try
            {
                await LiteDbScenario.RunAsync(runner).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                runner.Fail("[litedb] scenario aborted", ex);
            }
#endif

            Console.WriteLine();
            Console.WriteLine("RESULT: " + runner.Passed + " passed, " + runner.Failed + " failed");
            return runner.Failed == 0 ? 0 : 1;
        }
    }
}
