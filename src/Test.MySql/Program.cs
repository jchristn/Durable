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
            Console.WriteLine("  Test.MySql.exe --server <host> --database <db> --user <username> --password <pwd> [--port <port>]");
            Console.WriteLine("  Test.MySql.exe --connection-string <connection-string>");
            Console.WriteLine("  Test.MySql.exe --help");
            Console.WriteLine();
            Console.WriteLine("OPTIONS:");
            Console.WriteLine("  --server, -s          MySQL server hostname (default: localhost)");
            Console.WriteLine("  --port, -p            MySQL server port (default: 3306)");
            Console.WriteLine("  --database, -d        Database name (default: durable_test)");
            Console.WriteLine("  --user, -u            MySQL username (default: test_user)");
            Console.WriteLine("  --password, -pw       MySQL password (default: test_password)");
            Console.WriteLine("  --connection-string   Full MySQL connection string");
            Console.WriteLine("  --help, -h, /?        Show this help message");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  # Use default connection (localhost, durable_test, test_user, test_password)");
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
            Console.WriteLine("      -e MYSQL_USER=test_user \\");
            Console.WriteLine("      -e MYSQL_PASSWORD=test_password \\");
            Console.WriteLine("      -p 3306:3306 \\");
            Console.WriteLine("      mysql:8.0");
            Console.WriteLine();
            Console.WriteLine("  Then run: Test.MySql.exe (uses default connection)");
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
            string server = "localhost";
            string port = "3306";
            string database = "durable_test";
            string user = "test_user";
            string password = "test_password";

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
