namespace Test.Sqlite
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for SQLite test suite execution.
    /// </summary>
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            if (args.Contains("--help") || args.Contains("-h") || args.Contains("/?"))
            {
                ShowUsage();
                return 0;
            }

            try
            {
                string connectionString = BuildConnectionString(args);

                using SqliteRepositoryProvider provider = new SqliteRepositoryProvider(connectionString);
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

        static void ShowUsage()
        {
            Console.WriteLine("====================================================");
            Console.WriteLine("       SQLITE INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();
            Console.WriteLine("USAGE:");
            Console.WriteLine("  Test.Sqlite.exe [--database-file <path>] [--connection-string <connection-string>]");
            Console.WriteLine("  Test.Sqlite.exe --help");
            Console.WriteLine();
            Console.WriteLine("OPTIONS:");
            Console.WriteLine("  --database-file, -f   SQLite database file path (default: in-memory database)");
            Console.WriteLine("  --connection-string   Full SQLite connection string");
            Console.WriteLine("  --help, -h, /?        Show this help message");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  # Use in-memory database (default, no database file required)");
            Console.WriteLine("  Test.Sqlite.exe");
            Console.WriteLine();
            Console.WriteLine("  # Use a file-based database");
            Console.WriteLine("  Test.Sqlite.exe --database-file test.db");
            Console.WriteLine();
            Console.WriteLine("  # Use full connection string");
            Console.WriteLine("  Test.Sqlite.exe --connection-string \"Data Source=test.db;Mode=ReadWriteCreate\"");
            Console.WriteLine();
            Console.WriteLine("NOTES:");
            Console.WriteLine("  - SQLite requires no separate database server installation");
            Console.WriteLine("  - By default, tests run against an in-memory database");
            Console.WriteLine("  - File-based databases are created automatically if they don't exist");
            Console.WriteLine("  - In-memory databases are faster but data is lost when the program exits");
            Console.WriteLine();
        }

        static string BuildConnectionString(string[] args)
        {
            // Check for full connection string first
            for (int i = 0; i < args.Length - 1; i++)
            {
                if (args[i] == "--connection-string")
                {
                    return args[i + 1];
                }
            }

            // Check for database file
            for (int i = 0; i < args.Length - 1; i++)
            {
                if (args[i] == "--database-file" || args[i] == "-f")
                {
                    string dbFile = args[i + 1];
                    return $"Data Source={dbFile}";
                }
            }

            // Default to in-memory database
            return null; // null will use default in-memory connection string
        }
    }
}
