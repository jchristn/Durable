namespace Test.Postgres
{
    using System;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for PostgreSQL test suite execution.
    /// </summary>
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            try
            {
                using PostgresRepositoryProvider provider = new PostgresRepositoryProvider();
                int exitCode = await SharedTestRunner.RunAllTestsAsync(provider);
                return exitCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                return 1;
            }
        }
    }
}
