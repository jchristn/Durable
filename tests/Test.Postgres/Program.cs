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

            try
            {
                // Run basic connectivity test
                var integrationTests = new PostgresIntegrationTests();

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
                Console.WriteLine("🎉 All PostgreSQL tests passed including advanced aggregations and collection operations!");

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
    }
}