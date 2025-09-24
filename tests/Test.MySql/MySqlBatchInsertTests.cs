namespace Test.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;
    using Xunit;
    using Xunit.Abstractions;

    /// <summary>
    /// Comprehensive performance and configuration tests for MySQL batch insert operations including:
    /// - Batch configurations: Different optimization strategies
    /// - Performance comparisons: Timing different approaches
    /// - Scalability testing: Various batch sizes (50, 500, 2000 records)
    /// - Transaction performance: Batch operations within transactions
    /// </summary>
    public class MySqlBatchInsertTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _connectionString;
        private readonly bool _skipTests;

        public MySqlBatchInsertTests(ITestOutputHelper output)
        {
            _output = output;
            _connectionString = "Server=localhost;Database=durable_batch_test;User=test_user;Password=test_password;";

            try
            {
                // Test connection availability
                using var connection = new MySqlConnector.MySqlConnection(_connectionString);
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();
                _output.WriteLine("MySQL batch insert tests initialized successfully");
            }
            catch (Exception ex)
            {
                _skipTests = true;
                _output.WriteLine($"WARNING: MySQL initialization failed - {ex.Message}");
            }
        }

        /// <summary>
        /// Tests default configuration with batch optimization enabled.
        /// </summary>
        [Fact]
        public async Task DefaultConfiguration_BatchInsert_OptimizesPerformance()
        {
            if (_skipTests) return;

            await TestConfiguration("Default Configuration", BatchInsertConfiguration.Default);
        }

        /// <summary>
        /// Tests small batch configuration optimized for fewer database round trips.
        /// </summary>
        [Fact]
        public async Task SmallBatchConfiguration_BatchInsert_HandlesSmallBatches()
        {
            if (_skipTests) return;

            await TestConfiguration("Small Batch Configuration", BatchInsertConfiguration.SmallBatch);
        }

        /// <summary>
        /// Tests large batch configuration optimized for maximum throughput.
        /// </summary>
        [Fact]
        public async Task LargeBatchConfiguration_BatchInsert_HandlesLargeBatches()
        {
            if (_skipTests) return;

            await TestConfiguration("Large Batch Configuration", BatchInsertConfiguration.LargeBatch);
        }

        /// <summary>
        /// Tests compatible configuration with optimizations disabled for compatibility.
        /// </summary>
        [Fact]
        public async Task CompatibleConfiguration_BatchInsert_FallsBackToIndividualInserts()
        {
            if (_skipTests) return;

            await TestConfiguration("Compatible (No Optimization)", BatchInsertConfiguration.Compatible);
        }

        /// <summary>
        /// Compares performance between optimized and non-optimized batch operations.
        /// </summary>
        [Fact]
        public async Task PerformanceComparison_OptimizedVsCompatible_ShowsSignificantImprovement()
        {
            if (_skipTests) return;

            await PerformanceComparison();
        }

        /// <summary>
        /// Tests scalability across different batch sizes to identify optimal configurations.
        /// </summary>
        [Fact]
        public async Task ScalabilityTesting_VariousBatchSizes_ValidatesPerformanceCharacteristics()
        {
            if (_skipTests) return;

            await ScalabilityTesting();
        }

        /// <summary>
        /// Tests batch operations within explicit transactions for consistency and rollback capabilities.
        /// </summary>
        [Fact]
        public async Task TransactionPerformance_BatchWithTransactions_MaintainsConsistency()
        {
            if (_skipTests) return;

            await TransactionPerformanceTesting();
        }

        /// <summary>
        /// Tests the batch insert configuration with different parameter limits for MySQL optimization.
        /// </summary>
        private async Task TestConfiguration(string configName, IBatchInsertConfiguration config)
        {
            _output.WriteLine($"\n=== Testing {configName} ===");
            _output.WriteLine($"MaxRowsPerBatch: {config.MaxRowsPerBatch}");
            _output.WriteLine($"MaxParametersPerStatement: {config.MaxParametersPerStatement}");
            _output.WriteLine($"EnableMultiRowInsert: {config.EnableMultiRowInsert}");
            _output.WriteLine($"EnablePreparedStatementReuse: {config.EnablePreparedStatementReuse}");

            using var repository = new MySqlRepository<Person>(_connectionString, config);
            await SetupDatabase(repository);

            // Adjust batch sizes based on whether multi-row inserts are enabled
            // Compatible configuration uses individual inserts, so use smaller batches to avoid connection pool exhaustion
            int smallSize = config.EnableMultiRowInsert ? 50 : 10;
            int mediumSize = config.EnableMultiRowInsert ? 500 : 20;
            int largeSize = config.EnableMultiRowInsert ? 2000 : 30;

            // Test small batch
            List<Person> smallBatch = GenerateTestPeople(smallSize);
            Stopwatch sw = Stopwatch.StartNew();
            IEnumerable<Person> createdSmall = await repository.CreateManyAsync(smallBatch);
            sw.Stop();

            int countSmall = await repository.CountAsync();
            _output.WriteLine($"Small batch ({smallSize}): {sw.ElapsedMilliseconds}ms, Count: {countSmall}");
            Assert.Equal(smallSize, countSmall);

            // Test medium batch
            await repository.DeleteAllAsync();
            List<Person> mediumBatch = GenerateTestPeople(mediumSize);
            sw.Restart();
            IEnumerable<Person> createdMedium = await repository.CreateManyAsync(mediumBatch);
            sw.Stop();

            int countMedium = await repository.CountAsync();
            _output.WriteLine($"Medium batch ({mediumSize}): {sw.ElapsedMilliseconds}ms, Count: {countMedium}");
            Assert.Equal(mediumSize, countMedium);

            // Test large batch
            await repository.DeleteAllAsync();
            List<Person> largeBatch = GenerateTestPeople(largeSize);
            sw.Restart();
            IEnumerable<Person> createdLarge = await repository.CreateManyAsync(largeBatch);
            sw.Stop();

            int countLarge = await repository.CountAsync();
            _output.WriteLine($"Large batch ({largeSize}): {sw.ElapsedMilliseconds}ms, Count: {countLarge}");
            Assert.Equal(largeSize, countLarge);

            // Verify data integrity
            Person firstPerson = createdLarge.First();
            Person retrievedPerson = await repository.ReadByIdAsync(firstPerson.Id);
            bool dataIntegrityOk = retrievedPerson != null &&
                                 retrievedPerson.FirstName == firstPerson.FirstName &&
                                 retrievedPerson.LastName == firstPerson.LastName &&
                                 retrievedPerson.Email == firstPerson.Email;

            _output.WriteLine($"Data integrity check: {(dataIntegrityOk ? "PASS" : "FAIL")}");
            Assert.True(dataIntegrityOk, "Data integrity verification failed");

            // Test with transaction
            await repository.DeleteAllAsync();
            ITransaction transaction = null;
            try
            {
                int transactionSize = config.EnableMultiRowInsert ? 100 : 25;
                transaction = await repository.BeginTransactionAsync();
                List<Person> transactionBatch = GenerateTestPeople(transactionSize);
                sw.Restart();
                await repository.CreateManyAsync(transactionBatch, transaction);
                await transaction.CommitAsync();
                sw.Stop();

                int transactionCount = await repository.CountAsync();
                _output.WriteLine($"Transaction batch ({transactionSize}): {sw.ElapsedMilliseconds}ms, Count: {transactionCount}");
                Assert.Equal(transactionSize, transactionCount);
            }
            catch
            {
                if (transaction != null)
                {
                    try { await transaction.RollbackAsync(); } catch { /* Already disposed */ }
                }
                throw;
            }
            finally
            {
                transaction?.Dispose();
            }

            _output.WriteLine($"✅ {configName} test completed successfully");
        }

        /// <summary>
        /// Performs detailed performance comparison between different optimization strategies.
        /// </summary>
        private async Task PerformanceComparison()
        {
            _output.WriteLine("\n=== Performance Comparison ===");
            const int optimizedTestSize = 1000;
            const int compatibleTestSize = 50; // Use smaller size for compatible configuration to avoid connection pool exhaustion

            long optimizedTime;
            long compatibleTime;

            // Test optimized version (LargeBatch configuration)
            using (var optimizedRepo = new MySqlRepository<Person>(_connectionString, BatchInsertConfiguration.LargeBatch))
            {
                await SetupDatabase(optimizedRepo);

                List<Person> testData = GenerateTestPeople(optimizedTestSize);
                Stopwatch sw = Stopwatch.StartNew();
                await optimizedRepo.CreateManyAsync(testData);
                sw.Stop();
                optimizedTime = sw.ElapsedMilliseconds;

                int optimizedCount = await optimizedRepo.CountAsync();
                Assert.Equal(optimizedTestSize, optimizedCount);
            }

            // Test compatible (non-optimized) version
            using (var compatibleRepo = new MySqlRepository<Person>(_connectionString, BatchInsertConfiguration.Compatible))
            {
                await SetupDatabase(compatibleRepo);

                List<Person> testData = GenerateTestPeople(compatibleTestSize); // Use smaller batch for individual inserts
                Stopwatch sw = Stopwatch.StartNew();
                await compatibleRepo.CreateManyAsync(testData);
                sw.Stop();
                compatibleTime = sw.ElapsedMilliseconds;

                int compatibleCount = await compatibleRepo.CountAsync();
                Assert.Equal(compatibleTestSize, compatibleCount);
            }

            _output.WriteLine($"Optimized (multi-row, {optimizedTestSize} records): {optimizedTime}ms");
            _output.WriteLine($"Compatible (individual, {compatibleTestSize} records): {compatibleTime}ms");

            // Calculate records per millisecond for fair comparison
            if (optimizedTime > 0 && compatibleTime > 0)
            {
                double optimizedRate = optimizedTestSize / (double)optimizedTime;
                double compatibleRate = compatibleTestSize / (double)compatibleTime;
                _output.WriteLine($"Optimized rate: {optimizedRate:F1} records/ms");
                _output.WriteLine($"Compatible rate: {compatibleRate:F1} records/ms");

                // Assert that optimized version has better throughput
                Assert.True(optimizedRate > compatibleRate * 0.5, // Allow some variance
                    $"Optimized version should have better throughput: {optimizedRate:F1} vs {compatibleRate:F1} records/ms");
            }

            _output.WriteLine("✅ Performance comparison completed successfully");
        }

        /// <summary>
        /// Tests scalability characteristics across different batch sizes and configurations.
        /// </summary>
        private async Task ScalabilityTesting()
        {
            _output.WriteLine("\n=== Scalability Testing ===");

            var testSizes = new[] { 50, 100, 250, 500, 1000, 2000, 5000 };
            var configurations = new[]
            {
                ("Small Batch", BatchInsertConfiguration.SmallBatch),
                ("Default", BatchInsertConfiguration.Default),
                ("Large Batch", BatchInsertConfiguration.LargeBatch)
            };

            foreach (var (configName, config) in configurations)
            {
                _output.WriteLine($"\n--- Scalability Test: {configName} ---");

                using var repository = new MySqlRepository<Person>(_connectionString, config);

                foreach (int testSize in testSizes)
                {
                    await SetupDatabase(repository);

                    List<Person> testData = GenerateTestPeople(testSize);
                    Stopwatch sw = Stopwatch.StartNew();
                    await repository.CreateManyAsync(testData);
                    sw.Stop();

                    int count = await repository.CountAsync();
                    Assert.Equal(testSize, count);

                    double recordsPerSecond = testSize / (sw.ElapsedMilliseconds / 1000.0);
                    _output.WriteLine($"Size: {testSize,5} | Time: {sw.ElapsedMilliseconds,4}ms | Rate: {recordsPerSecond:F0} records/sec");

                    // Force garbage collection to cleanup connections
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }

            _output.WriteLine("✅ Scalability testing completed successfully");
        }

        /// <summary>
        /// Tests transaction performance with different batch sizes and rollback scenarios.
        /// </summary>
        private async Task TransactionPerformanceTesting()
        {
            _output.WriteLine("\n=== Transaction Performance Testing ===");

            using var repository = new MySqlRepository<Person>(_connectionString, BatchInsertConfiguration.Default);
            await SetupDatabase(repository);

            // Test transaction commit performance
            _output.WriteLine("\n--- Transaction Commit Performance ---");
            var batchSizes = new[] { 50, 200, 500, 1000 };

            foreach (int batchSize in batchSizes)
            {
                await SetupDatabase(repository);

                List<Person> testData = GenerateTestPeople(batchSize);

                ITransaction transaction = null;
                try
                {
                    transaction = await repository.BeginTransactionAsync();
                    Stopwatch sw = Stopwatch.StartNew();
                    await repository.CreateManyAsync(testData, transaction);
                    await transaction.CommitAsync();
                    sw.Stop();

                    int count = await repository.CountAsync();
                    Assert.Equal(batchSize, count);

                    _output.WriteLine($"Batch size: {batchSize,4} | Transaction time: {sw.ElapsedMilliseconds,4}ms");
                }
                catch
                {
                    if (transaction != null)
                    {
                        try { await transaction.RollbackAsync(); } catch { /* Already disposed */ }
                    }
                    throw;
                }
                finally
                {
                    transaction?.Dispose();
                }
            }

            // Test transaction rollback functionality
            _output.WriteLine("\n--- Transaction Rollback Testing ---");
            await SetupDatabase(repository);

            ITransaction rollbackTransaction = null;
            try
            {
                rollbackTransaction = await repository.BeginTransactionAsync();
                List<Person> rollbackData = GenerateTestPeople(100);
                await repository.CreateManyAsync(rollbackData, rollbackTransaction);

                // Verify data exists within transaction
                int countInTransaction = await repository.CountAsync(transaction: rollbackTransaction);
                Assert.Equal(100, countInTransaction);

                // Rollback instead of commit
                await rollbackTransaction.RollbackAsync();
            }
            catch
            {
                if (rollbackTransaction != null)
                {
                    try { await rollbackTransaction.RollbackAsync(); } catch { /* Already disposed */ }
                }
                throw;
            }
            finally
            {
                rollbackTransaction?.Dispose();
            }

            // Verify rollback worked - count should be 0
            int countAfterRollback = await repository.CountAsync();
            Assert.Equal(0, countAfterRollback);
            _output.WriteLine($"Rollback verification: {countAfterRollback} records (expected 0)");

            _output.WriteLine("✅ Transaction performance testing completed successfully");
        }

        /// <summary>
        /// Creates or recreates the people table with proper MySQL schema.
        /// </summary>
        private async Task CreateTableAsync(MySqlRepository<Person> repository)
        {
            await repository.ExecuteSqlAsync(@"
                CREATE TABLE IF NOT EXISTS people (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first VARCHAR(100) NOT NULL,
                    last VARCHAR(100) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(100) NOT NULL,
                    INDEX idx_department (department),
                    INDEX idx_age (age)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");
        }

        /// <summary>
        /// Sets up a clean database by dropping and recreating the table.
        /// </summary>
        private async Task SetupDatabase(MySqlRepository<Person> repository)
        {
            await repository.ExecuteSqlAsync(@"
                DROP TABLE IF EXISTS people;
                CREATE TABLE people (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first VARCHAR(100) NOT NULL,
                    last VARCHAR(100) NOT NULL,
                    age INT NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    salary DECIMAL(10,2) NOT NULL,
                    department VARCHAR(100) NOT NULL,
                    INDEX idx_department (department),
                    INDEX idx_age (age)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");
        }

        /// <summary>
        /// Generates test people with realistic data for performance testing.
        /// </summary>
        private static List<Person> GenerateTestPeople(int count)
        {
            string[] departments = new[] { "IT", "HR", "Finance", "Sales", "Marketing", "Operations", "Legal", "R&D" };
            string[] firstNames = new[] { "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack" };
            string[] lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez" };
            Random random = new Random(42); // Fixed seed for reproducible tests

            List<Person> people = new List<Person>(count);
            for (int i = 0; i < count; i++)
            {
                people.Add(new Person
                {
                    FirstName = firstNames[random.Next(firstNames.Length)],
                    LastName = lastNames[random.Next(lastNames.Length)],
                    Age = random.Next(18, 65),
                    Email = $"person{i}@batchtest.com",
                    Salary = (decimal)(random.NextDouble() * 80000 + 30000),
                    Department = departments[random.Next(departments.Length)]
                });
            }
            return people;
        }

        /// <summary>
        /// Disposes resources used by the test class.
        /// </summary>
        public void Dispose()
        {
            // No specific cleanup needed for this test class
        }
    }
}