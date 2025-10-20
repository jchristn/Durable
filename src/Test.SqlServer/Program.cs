namespace Test.SqlServer
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Entry point for SQL Server test suite execution.
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

                using SqlServerRepositoryProvider provider = new SqlServerRepositoryProvider(connectionString);
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
            Console.WriteLine("     SQL SERVER INTEGRATION TEST SUITE");
            Console.WriteLine("====================================================");
            Console.WriteLine();
            Console.WriteLine("USAGE:");
            Console.WriteLine("  Test.SqlServer.exe --server <host> --database <db> [--user <username>] [--password <pwd>] [--port <port>]");
            Console.WriteLine("  Test.SqlServer.exe --connection-string <connection-string>");
            Console.WriteLine("  Test.SqlServer.exe --help");
            Console.WriteLine();
            Console.WriteLine("OPTIONS:");
            Console.WriteLine("  --server, -s          SQL Server hostname (default: localhost)");
            Console.WriteLine("  --port, -p            SQL Server port (default: 1433)");
            Console.WriteLine("  --database, -d        Database name (default: durable_test)");
            Console.WriteLine("  --user, -u            SQL Server username (optional, uses Windows auth if not specified)");
            Console.WriteLine("  --password, -pw       SQL Server password (required if --user is specified)");
            Console.WriteLine("  --trusted             Use Windows Integrated Authentication (default if no user specified)");
            Console.WriteLine("  --connection-string   Full SQL Server connection string");
            Console.WriteLine("  --help, -h, /?        Show this help message");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  # Use Windows Authentication (default)");
            Console.WriteLine("  Test.SqlServer.exe");
            Console.WriteLine();
            Console.WriteLine("  # Use SQL Server Authentication");
            Console.WriteLine("  Test.SqlServer.exe --server myserver.com --user sa --password MyPassword123");
            Console.WriteLine();
            Console.WriteLine("  # Use full connection string");
            Console.WriteLine("  Test.SqlServer.exe --connection-string \"Server=localhost;Database=durable_test;Trusted_Connection=True;\"");
            Console.WriteLine();
            Console.WriteLine("SETUP INSTRUCTIONS:");
            Console.WriteLine("  If SQL Server is not installed, you can use Docker:");
            Console.WriteLine();
            Console.WriteLine("    docker run -d --name durable-sqlserver-test \\");
            Console.WriteLine("      -e 'ACCEPT_EULA=Y' \\");
            Console.WriteLine("      -e 'SA_PASSWORD=YourStrong@Passw0rd' \\");
            Console.WriteLine("      -p 1433:1433 \\");
            Console.WriteLine("      mcr.microsoft.com/mssql/server:2022-latest");
            Console.WriteLine();
            Console.WriteLine("    # Create database");
            Console.WriteLine("    docker exec durable-sqlserver-test /opt/mssql-tools/bin/sqlcmd \\");
            Console.WriteLine("      -S localhost -U sa -P 'YourStrong@Passw0rd' \\");
            Console.WriteLine("      -Q \"CREATE DATABASE durable_test\"");
            Console.WriteLine();
            Console.WriteLine("  Then run: Test.SqlServer.exe --user sa --password YourStrong@Passw0rd");
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
            string port = "1433";
            string database = "durable_test";
            string user = null;
            string password = null;
            bool useTrustedConnection = true;

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
                        useTrustedConnection = false;
                        break;
                    case "--password":
                    case "-pw":
                        password = args[i + 1];
                        break;
                }
            }

            if (args.Contains("--trusted"))
            {
                useTrustedConnection = true;
            }

            string serverWithPort = port != "1433" ? $"{server},{port}" : server;

            if (useTrustedConnection)
            {
                return $"Server={serverWithPort};Database={database};Trusted_Connection=True;TrustServerCertificate=True;";
            }
            else
            {
                return $"Server={serverWithPort};Database={database};User Id={user};Password={password};TrustServerCertificate=True;";
            }
        }
    }
}
