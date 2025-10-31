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

            try
            {
                string connectionString;

                // If no arguments provided, use interactive mode
                if (args.Length == 0)
                {
                    connectionString = PromptForConnectionParameters();
                }
                else
                {
                    connectionString = BuildConnectionString(args);
                }

                if (string.IsNullOrEmpty(connectionString))
                {
                    ShowUsage();
                    return 1;
                }

                // Debug mode
                if (args.Contains("--debug"))
                {
                    using PostgresRepositoryProvider debugProvider = new PostgresRepositoryProvider(connectionString);
                    await debugProvider.SetupDatabaseAsync();
                    return 0;
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
            Console.WriteLine("  Test.Postgres.exe                                                                 (interactive mode)");
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
            Console.WriteLine("INTERACTIVE MODE:");
            Console.WriteLine("  When run without arguments, you will be prompted for connection details.");
            Console.WriteLine("  Press Enter to accept default values shown in brackets.");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  # Interactive mode - prompts for connection details");
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
            Console.WriteLine("  Then run: Test.Postgres.exe (interactive mode with defaults)");
            Console.WriteLine();
        }

        static string PromptForConnectionParameters()
        {
            Console.WriteLine("====================================================");
            Console.WriteLine("     POSTGRESQL INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();
            Console.WriteLine("No connection parameters provided. Please enter connection details:");
            Console.WriteLine("(Press Enter to use default values shown in brackets)");
            Console.WriteLine("(For password: Enter 'none' or leave blank for no password)");
            Console.WriteLine();

            string host = PromptWithDefault("Host", "localhost");
            string port = PromptWithDefault("Port", "5432");
            string database = PromptWithDefault("Database", "durable_test");
            string username = PromptWithDefault("Username", "test_user");
            string password = PromptWithOptional("Password", "test_password");

            string connectionString;
            if (string.IsNullOrEmpty(password))
            {
                connectionString = $"Host={host};Port={port};Database={database};Username={username};";
            }
            else
            {
                connectionString = $"Host={host};Port={port};Database={database};Username={username};Password={password};";
            }

            Console.WriteLine();
            Console.WriteLine($"Connection string: {connectionString}");
            Console.WriteLine();
            Console.WriteLine("Press Enter to start tests or Ctrl+C to cancel...");
            Console.ReadLine();
            Console.WriteLine();

            return connectionString;
        }

        static string PromptWithDefault(string prompt, string defaultValue)
        {
            Console.Write($"{prompt} [{defaultValue}]: ");
            string? input = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(input))
            {
                return defaultValue;
            }

            return input.Trim();
        }

        static string PromptWithOptional(string prompt, string defaultValue)
        {
            Console.Write($"{prompt} [{defaultValue}, or 'none' for no password]: ");
            string? input = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(input))
            {
                return defaultValue;
            }

            string trimmedInput = input.Trim();

            if (trimmedInput.Equals("none", StringComparison.OrdinalIgnoreCase))
            {
                return string.Empty;
            }

            return trimmedInput;
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
