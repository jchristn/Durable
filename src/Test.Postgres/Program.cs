using System;
using System.Threading.Tasks;

namespace Test.Postgres
{
    /// <summary>
    /// Entry point for the PostgreSQL test console application.
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// Main entry point for the test console application.
        /// </summary>
        /// <param name="args">Command line arguments</param>
        /// <returns>Exit code (0 for success)</returns>
        public static async Task<int> Main(string[] args)
        {
            Console.WriteLine("PostgreSQL Durable ORM Test Suite");
            Console.WriteLine("==================================");
            Console.WriteLine();

            string connectionString;

            // Parse command line arguments with priority: CLI args > Environment vars > Interactive prompt > Default
            if (args.Length > 0)
            {
                connectionString = args[0];
                Console.WriteLine($"Using connection string from command line: {MaskConnectionString(connectionString)}\n");
            }
            else
            {
                connectionString = BuildConnectionString();
                Console.WriteLine("Tip: You can specify a custom PostgreSQL connection string by passing it as an argument.");
                Console.WriteLine("     Example: dotnet Test.Postgres.dll \"Host=localhost;Database=mydb;Username=myuser;Password=mypass;\"\n");
            }

            try
            {
                // Run basic connectivity test
                var integrationTests = new PostgresIntegrationTests(connectionString);

                Console.WriteLine("Testing PostgreSQL connectivity...");
                await integrationTests.CanConnectToDatabase();
                Console.WriteLine("✅ Database connectivity test passed");

                Console.WriteLine();
                Console.WriteLine("Testing sanitizer functionality...");
                integrationTests.PostgresSanitizerWorksCorrectly();
                Console.WriteLine("✅ Sanitizer test passed");

                Console.WriteLine();
                Console.WriteLine("Testing connection factory extensions...");
                integrationTests.ConnectionFactoryExtensionsWork();
                Console.WriteLine("✅ Connection factory extensions test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL infrastructure...");
                integrationTests.PostgresInfrastructureIsSetup();
                Console.WriteLine("✅ PostgreSQL infrastructure test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL aggregation methods...");
                await integrationTests.PostgresAggregationMethodsWorkCorrectly();
                Console.WriteLine("✅ Aggregation methods test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL aggregation methods with transactions...");
                await integrationTests.PostgresAggregationMethodsWorkWithTransactions();
                Console.WriteLine("✅ Aggregation with transactions test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL aggregation error handling...");
                integrationTests.PostgresAggregationMethodsHandleErrorsCorrectly();
                Console.WriteLine("✅ Aggregation error handling test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL collection operations...");
                await integrationTests.PostgresCollectionOperationsWorkCorrectly();
                Console.WriteLine("✅ Collection operations test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL collection operations with transactions...");
                await integrationTests.PostgresCollectionOperationsWorkWithTransactions();
                Console.WriteLine("✅ Collection operations with transactions test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL collection operations edge cases...");
                await integrationTests.PostgresCollectionOperationsHandleEdgeCases();
                Console.WriteLine("✅ Collection operations edge cases test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL collection operations cancellation support...");
                await integrationTests.PostgresCollectionOperationsSupportCancellation();
                Console.WriteLine("✅ Collection operations cancellation test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL specialized update operations...");
                await integrationTests.PostgresSpecializedUpdateOperationsWorkCorrectly();
                Console.WriteLine("✅ Specialized update operations test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL specialized update operations with transactions...");
                await integrationTests.PostgresSpecializedUpdateOperationsWorkWithTransactions();
                Console.WriteLine("✅ Specialized update operations with transactions test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL specialized update operations error handling...");
                await integrationTests.PostgresSpecializedUpdateOperationsHandleErrorsCorrectly();
                Console.WriteLine("✅ Specialized update operations error handling test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL upsert operations...");
                await integrationTests.PostgresUpsertOperationsWorkCorrectly();
                Console.WriteLine("✅ Upsert operations test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL upsert operations with transactions...");
                await integrationTests.PostgresUpsertOperationsWorkWithTransactions();
                Console.WriteLine("✅ Upsert operations with transactions test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL upsert operations error handling...");
                await integrationTests.PostgresUpsertOperationsHandleErrorsCorrectly();
                Console.WriteLine("✅ Upsert operations error handling test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL Select projections...");
                await integrationTests.PostgresSelectProjectionsWorkCorrectly();
                Console.WriteLine("✅ Select projections test passed");

                Console.WriteLine();
                Console.WriteLine("Testing PostgreSQL Include operations...");
                await integrationTests.PostgresIncludeOperationsWorkCorrectly();
                Console.WriteLine("✅ Include operations test passed");

                Console.WriteLine();
                Console.WriteLine("🎉 All PostgreSQL tests passed including advanced aggregations, collection operations, specialized update operations, upsert operations, Select projections, and Include operations!");

                integrationTests.Dispose();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Test failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                return 1;
            }
        }

        /// <summary>
        /// Builds the connection string from environment variables or prompts user.
        /// </summary>
        /// <returns>A PostgreSQL connection string.</returns>
        private static string BuildConnectionString()
        {
            string host = Environment.GetEnvironmentVariable("POSTGRES_HOST") ?? "";
            string database = Environment.GetEnvironmentVariable("POSTGRES_DATABASE") ?? "";
            string username = Environment.GetEnvironmentVariable("POSTGRES_USER") ?? "";
            string password = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD") ?? "";

            // If any required value is missing, prompt for all of them
            if (string.IsNullOrEmpty(host) || string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
            {
                Console.WriteLine("=== PostgreSQL Connection Setup ===");

                if (string.IsNullOrEmpty(host))
                {
                    Console.Write("Enter PostgreSQL host and port (e.g., 'localhost' or 'server.com:5432'): ");
                    Console.Write("(or press Enter for default 'localhost'): ");
                    host = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(host))
                    {
                        host = "localhost";
                    }
                }
                else
                {
                    Console.WriteLine($"Using host from environment: {host}");
                }

                if (string.IsNullOrEmpty(username))
                {
                    Console.Write("Enter PostgreSQL username (or press Enter for default 'test_user'): ");
                    username = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(username))
                    {
                        username = "test_user";
                    }
                }
                else
                {
                    Console.WriteLine($"Using username from environment: {username}");
                }

                if (string.IsNullOrEmpty(password))
                {
                    Console.Write("Enter PostgreSQL password (or press Enter for default 'test_password'): ");
                    password = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(password))
                    {
                        password = "test_password";
                    }
                }

                if (string.IsNullOrEmpty(database))
                {
                    Console.Write("Enter database name (or press Enter for default 'durable_integration_test'): ");
                    database = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(database))
                    {
                        database = "durable_integration_test";
                    }
                }
                else
                {
                    Console.WriteLine($"Using database from environment: {database}");
                }

                Console.WriteLine();
            }

            // Use defaults if still empty
            if (string.IsNullOrEmpty(database))
            {
                database = "durable_integration_test";
            }

            return $"Host={host};Database={database};Username={username};Password={password};";
        }

        /// <summary>
        /// Masks the password in a connection string for safe display.
        /// </summary>
        /// <param name="connectionString">The connection string to mask.</param>
        /// <returns>Connection string with password hidden.</returns>
        private static string MaskConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return connectionString;

            int passwordIndex = connectionString.IndexOf("Password=", StringComparison.OrdinalIgnoreCase);
            if (passwordIndex == -1)
                return connectionString;

            int passwordStart = passwordIndex + "Password=".Length;
            int semicolonIndex = connectionString.IndexOf(';', passwordStart);

            if (semicolonIndex == -1)
                return connectionString.Substring(0, passwordStart) + "***";

            return connectionString.Substring(0, passwordStart) + "***" + connectionString.Substring(semicolonIndex);
        }
    }
}