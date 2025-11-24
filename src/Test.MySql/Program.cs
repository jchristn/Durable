namespace Test.MySql
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for MySQL test suite execution.
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

                using MySqlRepositoryProvider provider = new MySqlRepositoryProvider(connectionString);
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
            Console.WriteLine("       MYSQL INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();
            Console.WriteLine("USAGE:");
            Console.WriteLine("  Test.MySql.exe                                                                 (interactive mode)");
            Console.WriteLine("  Test.MySql.exe --server <host> --database <db> --user <username> --password <pwd> [--port <port>]");
            Console.WriteLine("  Test.MySql.exe --connection-string <connection-string>");
            Console.WriteLine("  Test.MySql.exe --help");
            Console.WriteLine();
            Console.WriteLine("OPTIONS:");
            Console.WriteLine("  --server, -s          MySQL server hostname (default: localhost)");
            Console.WriteLine("  --port, -p            MySQL server port (default: 3306)");
            Console.WriteLine("  --database, -d        Database name (default: durable_test)");
            Console.WriteLine("  --user, -u            MySQL username (default: root)");
            Console.WriteLine("  --password, -pw       MySQL password (default: password)");
            Console.WriteLine("  --connection-string   Full MySQL connection string");
            Console.WriteLine("  --help, -h, /?        Show this help message");
            Console.WriteLine();
            Console.WriteLine("INTERACTIVE MODE:");
            Console.WriteLine("  When run without arguments, you will be prompted for connection details.");
            Console.WriteLine("  Press Enter to accept default values shown in brackets.");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  # Interactive mode - prompts for connection details");
            Console.WriteLine("  Test.MySql.exe");
            Console.WriteLine();
            Console.WriteLine("  # Specify custom server and credentials");
            Console.WriteLine("  Test.MySql.exe --server myserver.com --user admin --password secret123");
            Console.WriteLine();
            Console.WriteLine("  # Use full connection string");
            Console.WriteLine("  Test.MySql.exe --connection-string \"Server=localhost;Database=durable_test;User=test_user;Password=test_password;\"");
            Console.WriteLine();
            Console.WriteLine("SETUP INSTRUCTIONS:");
            Console.WriteLine("  If MySQL is not installed, you can use Docker:");
            Console.WriteLine();
            Console.WriteLine("    docker run -d --name durable-mysql-test \\");
            Console.WriteLine("      -e MYSQL_ROOT_PASSWORD=root_password \\");
            Console.WriteLine("      -e MYSQL_DATABASE=durable_test \\");
            Console.WriteLine("      -e MYSQL_USER=root \\");
            Console.WriteLine("      -e MYSQL_PASSWORD=password \\");
            Console.WriteLine("      -p 3306:3306 \\");
            Console.WriteLine("      mysql:8.0");
            Console.WriteLine();
            Console.WriteLine("  Then run: Test.MySql.exe (interactive mode with defaults)");
            Console.WriteLine();
        }

        static string PromptForConnectionParameters()
        {
            Console.WriteLine("====================================================");
            Console.WriteLine("       MYSQL INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();
            Console.WriteLine("No connection parameters provided. Please enter connection details:");
            Console.WriteLine("(Press Enter to use default values shown in brackets)");
            Console.WriteLine("(For password: Enter 'none' or leave blank for no password)");
            Console.WriteLine();

            string server = PromptWithDefault("Server", "localhost");
            string port = PromptWithDefault("Port", "3306");
            string database = PromptWithDefault("Database", "durable_test");
            string user = PromptWithDefault("Username", "root");
            string password = PromptWithOptional("Password", "password");

            string connectionString;
            if (string.IsNullOrEmpty(password))
            {
                connectionString = $"Server={server};Port={port};Database={database};User={user};";
            }
            else
            {
                connectionString = $"Server={server};Port={port};Database={database};User={user};Password={password};";
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
            string server = "localhost";
            string port = "3306";
            string database = "durable_test";
            string user = "root";
            string password = "password";

            for (int i = 0; i < args.Length - 1; i++)
            {
                switch (args[i].ToLower())
                {
                    case "--server":
                    case "-s":
                        server = args[i + 1];
                        break;
                    case "--port":
                    case "-p":
                        port = args[i + 1];
                        break;
                    case "--database":
                    case "-d":
                        database = args[i + 1];
                        break;
                    case "--user":
                    case "-u":
                        user = args[i + 1];
                        break;
                    case "--password":
                    case "-pw":
                        password = args[i + 1];
                        break;
                }
            }

            return $"Server={server};Port={port};Database={database};User={user};Password={password};";
        }
    }
}
