namespace Test.Postgres
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for PostgreSQL test suite execution.
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

            // If no arguments provided, show usage and exit
            if (args.Length == 0)
            {
                ShowUsage();
                return 1;
            }

            try
            {
                string connectionString = BuildConnectionString(args);

                if (string.IsNullOrEmpty(connectionString))
                {
                    ShowUsage();
                    return 1;
                }

                using PostgresRepositoryProvider provider = new PostgresRepositoryProvider(connectionString);
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
            Console.WriteLine("     POSTGRESQL INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();
            Console.WriteLine("USAGE:");
            Console.WriteLine("  Test.Postgres.exe --host <host> --database <db> --username <user> --password <pwd> [--port <port>]");
            Console.WriteLine("  Test.Postgres.exe --connection-string <connection-string>");
            Console.WriteLine("  Test.Postgres.exe --help");
            Console.WriteLine();
            Console.WriteLine("OPTIONS:");
            Console.WriteLine("  --host, -h            PostgreSQL server hostname (default: localhost)");
            Console.WriteLine("  --port, -p            PostgreSQL server port (default: 5432)");
            Console.WriteLine("  --database, -d        Database name (default: durable_test)");
            Console.WriteLine("  --username, -u        PostgreSQL username (default: test_user)");
            Console.WriteLine("  --password, -pw       PostgreSQL password (default: test_password)");
            Console.WriteLine("  --connection-string   Full PostgreSQL connection string");
            Console.WriteLine("  --help, -h, /?        Show this help message");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  # Use default connection (localhost, durable_test, test_user, test_password)");
            Console.WriteLine("  Test.Postgres.exe");
            Console.WriteLine();
            Console.WriteLine("  # Specify custom server and credentials");
            Console.WriteLine("  Test.Postgres.exe --host myserver.com --username admin --password secret123");
            Console.WriteLine();
            Console.WriteLine("  # Use full connection string");
            Console.WriteLine("  Test.Postgres.exe --connection-string \"Host=localhost;Database=durable_test;Username=test_user;Password=test_password;\"");
            Console.WriteLine();
            Console.WriteLine("SETUP INSTRUCTIONS:");
            Console.WriteLine("  If PostgreSQL is not installed, you can use Docker:");
            Console.WriteLine();
            Console.WriteLine("    docker run -d --name durable-postgres-test \\");
            Console.WriteLine("      -e POSTGRES_DB=durable_test \\");
            Console.WriteLine("      -e POSTGRES_USER=test_user \\");
            Console.WriteLine("      -e POSTGRES_PASSWORD=test_password \\");
            Console.WriteLine("      -p 5432:5432 \\");
            Console.WriteLine("      postgres:15");
            Console.WriteLine();
            Console.WriteLine("  Then run: Test.Postgres.exe (uses default connection)");
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

            // Build connection string from individual parameters
            string host = "localhost";
            string port = "5432";
            string database = "durable_test";
            string username = "test_user";
            string password = "test_password";

            for (int i = 0; i < args.Length - 1; i++)
            {
                switch (args[i].ToLower())
                {
                    case "--host":
                    case "-h":
                        host = args[i + 1];
                        break;
                    case "--port":
                    case "-p":
                        port = args[i + 1];
                        break;
                    case "--database":
                    case "-d":
                        database = args[i + 1];
                        break;
                    case "--username":
                    case "-u":
                        username = args[i + 1];
                        break;
                    case "--password":
                    case "-pw":
                        password = args[i + 1];
                        break;
                }
            }

            return $"Host={host};Port={port};Database={database};Username={username};Password={password};";
        }
    }
}
