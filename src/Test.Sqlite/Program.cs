namespace Test.Sqlite
{
    using System;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for SQLite test suite execution.
    /// </summary>
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            try
            {
                using SqliteRepositoryProvider provider = new SqliteRepositoryProvider();
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
