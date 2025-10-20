namespace Test.MySql
{
    using System;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for MySQL test suite execution.
    /// </summary>
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            try
            {
                using MySqlRepositoryProvider provider = new MySqlRepositoryProvider();
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
