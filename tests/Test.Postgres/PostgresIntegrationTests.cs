using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Durable;
using Durable.Postgres;
using Test.Shared;
using Npgsql;

namespace Test.Postgres
{
    /// <summary>
    /// Comprehensive integration tests for PostgreSQL implementation.
    /// These tests require a running PostgreSQL server and will be skipped if connection fails.
    ///
    /// To run these tests:
    /// 1. Ensure PostgreSQL server is running on localhost:5432
    /// 2. Create database: CREATE DATABASE durable_integration_test;
    /// 3. Create user: CREATE USER test_user WITH PASSWORD 'test_password';
    /// 4. Grant permissions: GRANT ALL PRIVILEGES ON DATABASE durable_integration_test TO test_user;
    /// </summary>
    public class PostgresIntegrationTests : IDisposable
    {

        #region Private-Members

        private const string TestConnectionString = "Host=localhost;Database=durable_integration_test;Username=test_user;Password=test_password;";
        private const string TestDatabaseName = "durable_integration_test";
        private static readonly object _TestLock = new object();
        private static bool _DatabaseSetupComplete = false;
        private static bool _SkipTests = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the test class by setting up the test database.
        /// </summary>
        public PostgresIntegrationTests()
        {
            lock (_TestLock)
            {
                if (!_DatabaseSetupComplete)
                {
                    SetupTestDatabase();
                    _DatabaseSetupComplete = true;
                }
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tests basic repository instantiation and interface compliance.
        /// </summary>
        [Fact]
        public void CanCreateRepository()
        {
            if (_SkipTests) return;

            // This test will fail until we implement PostgresRepository
            // For now, we'll just test the connection factory
            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);

            Assert.NotNull(connectionFactory);
            Assert.IsAssignableFrom<IConnectionFactory>(connectionFactory);
        }

        /// <summary>
        /// Tests database connectivity and basic SQL execution.
        /// </summary>
        [Fact]
        public async Task CanConnectToDatabase()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var connection = await connectionFactory.GetConnectionAsync();

            // Ensure connection is open (GetConnectionAsync might return an already open connection)
            if (connection.State != System.Data.ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            Assert.Equal(System.Data.ConnectionState.Open, connection.State);

            using var command = new NpgsqlCommand("SELECT version()", (NpgsqlConnection)connection);
            string result = await command.ExecuteScalarAsync() as string ?? string.Empty;

            Assert.NotNull(result);
            Assert.Contains("PostgreSQL", result);
        }

        /// <summary>
        /// Tests PostgreSQL-specific sanitizer functionality.
        /// </summary>
        [Fact]
        public void PostgresSanitizerWorksCorrectly()
        {
            var sanitizer = new PostgresSanitizer();

            // Test string sanitization
            string sanitized = sanitizer.SanitizeString("test'value");
            Assert.Equal("'test''value'", sanitized);

            // Test identifier sanitization
            string identifier = sanitizer.SanitizeIdentifier("test_column");
            Assert.Equal("test_column", identifier); // Should not quote simple identifiers

            string complexIdentifier = sanitizer.SanitizeIdentifier("test column");
            Assert.Equal("\"test column\"", complexIdentifier); // Should quote identifiers with spaces

            // Test null handling
            string nullValue = sanitizer.SanitizeString(null);
            Assert.Equal("NULL", nullValue);

            // Test boolean formatting (PostgreSQL uses true/false)
            string trueValue = sanitizer.FormatValue(true);
            Assert.Equal("true", trueValue);

            string falseValue = sanitizer.FormatValue(false);
            Assert.Equal("false", falseValue);
        }

        /// <summary>
        /// Tests PostgreSQL connection factory extensions.
        /// </summary>
        [Fact]
        public void ConnectionFactoryExtensionsWork()
        {
            // Test local factory creation
            using var localFactory = PostgresConnectionFactoryExtensions.CreateLocalPostgresFactory("test_db");
            Assert.NotNull(localFactory);

            // Test production factory creation
            using var prodFactory = PostgresConnectionFactoryExtensions.CreateProductionPostgresFactory(
                "localhost", "test_db", "user", "pass");
            Assert.NotNull(prodFactory);

            // Test Unix socket factory creation
            using var unixFactory = PostgresConnectionFactoryExtensions.CreateUnixSocketPostgresFactory("test_db");
            Assert.NotNull(unixFactory);
        }

        /// <summary>
        /// Tests basic PostgreSQL infrastructure setup.
        /// </summary>
        [Fact]
        public void PostgresInfrastructureIsSetup()
        {
            // This is a basic smoke test to ensure our infrastructure compiles and loads correctly
            var sanitizer = new PostgresSanitizer();
            using var connectionFactory = new PostgresConnectionFactory("Host=localhost;Database=test;Username=test;Password=test;");

            // Test that basic components were created successfully
            Assert.NotNull(sanitizer);
            Assert.NotNull(connectionFactory);

            // Test that we can access basic functionality
            string sanitized = sanitizer.SanitizeString("test");
            Assert.Equal("'test'", sanitized);
        }

        /// <summary>
        /// Tests PostgreSQL aggregation methods (Min, Max, Average, Sum) with sample data.
        /// </summary>
        [Fact]
        public async Task PostgresAggregationMethodsWorkCorrectly()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            // Create test table
            await CreateTestTableAsync(connectionFactory);

            try
            {
                // Insert test data using raw SQL to avoid entity mapping issues during testing phase
                await InsertTestDataAsync(connectionFactory);

                // Test Max aggregation
                decimal maxPrice = repository.Max(e => e.Price);
                Assert.True(maxPrice >= 0);

                // Test Min aggregation
                decimal minPrice = repository.Min(e => e.Price);
                Assert.True(minPrice >= 0);

                // Test Sum aggregation
                decimal totalPrice = repository.Sum(e => e.Price);
                Assert.True(totalPrice >= 0);

                // Test Average aggregation
                decimal avgPrice = repository.Average(e => e.Price);
                Assert.True(avgPrice >= 0);

                // Test async versions
                decimal maxPriceAsync = await repository.MaxAsync(e => e.Price);
                Assert.True(maxPriceAsync >= 0);

                decimal minPriceAsync = await repository.MinAsync(e => e.Price);
                Assert.True(minPriceAsync >= 0);

                decimal sumAsync = await repository.SumAsync(e => e.Price);
                Assert.True(sumAsync >= 0);

                decimal avgAsync = await repository.AverageAsync(e => e.Price);
                Assert.True(avgAsync >= 0);

                // Verify calculations are consistent
                Assert.Equal(maxPrice, maxPriceAsync);
                Assert.Equal(minPrice, minPriceAsync);
                Assert.Equal(totalPrice, sumAsync);
                Assert.Equal(avgPrice, avgAsync);

                // Test edge cases - empty result set
                decimal sumEmpty = repository.Sum(e => e.Price, e => e.Price < 0);
                Assert.Equal(0m, sumEmpty);

                decimal avgEmpty = repository.Average(e => e.Price, e => e.Price < 0);
                Assert.Equal(0m, avgEmpty);
            }
            finally
            {
                // Cleanup
                await DropTestTableAsync(connectionFactory);
            }
        }

        /// <summary>
        /// Tests PostgreSQL aggregation methods with transaction support.
        /// </summary>
        [Fact]
        public async Task PostgresAggregationMethodsWorkWithTransactions()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            await CreateTestTableAsync(connectionFactory);

            try
            {
                // Insert test data first
                await InsertTestDataAsync(connectionFactory);

                using var transaction = await repository.BeginTransactionAsync();

                // Test aggregations within transaction
                decimal maxPrice = repository.Max(e => e.Price, null, transaction);
                Assert.True(maxPrice >= 0);

                decimal sumPrice = await repository.SumAsync(e => e.Price, null, transaction);
                Assert.True(sumPrice >= 0);

                await transaction.CommitAsync();
            }
            finally
            {
                await DropTestTableAsync(connectionFactory);
            }
        }

        /// <summary>
        /// Tests edge cases and error handling for aggregation methods.
        /// </summary>
        [Fact]
        public void PostgresAggregationMethodsHandleErrorsCorrectly()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            // Test null selector argument
            Assert.Throws<ArgumentNullException>(() => repository.Max<decimal>(null!));
            Assert.Throws<ArgumentNullException>(() => repository.Min<decimal>(null!));
            Assert.Throws<ArgumentNullException>(() => repository.Average(null!));
            Assert.Throws<ArgumentNullException>(() => repository.Sum(null!));

            // Test async null selector arguments
            Assert.ThrowsAsync<ArgumentNullException>(() => repository.MaxAsync<decimal>(null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => repository.MinAsync<decimal>(null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => repository.AverageAsync(null!));
            Assert.ThrowsAsync<ArgumentNullException>(() => repository.SumAsync(null!));
        }

        /// <summary>
        /// Tests PostgreSQL collection operations (ReadMany, ReadAll) with predicates and filtering.
        /// </summary>
        [Fact]
        public async Task PostgresCollectionOperationsWorkCorrectly()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            // Create test table
            await CreateTestTableAsync(connectionFactory);

            try
            {
                // Insert test data
                await InsertTestDataAsync(connectionFactory);

                // Test ReadAll synchronous
                IEnumerable<ComplexEntity> allEntities = repository.ReadAll();
                var allEntitiesList = allEntities.ToList();
                Assert.True(allEntitiesList.Count >= 5);

                // Test ReadMany without predicate (should be same as ReadAll)
                IEnumerable<ComplexEntity> allEntitiesFromReadMany = repository.ReadMany();
                var allFromReadManyList = allEntitiesFromReadMany.ToList();
                Assert.Equal(allEntitiesList.Count, allFromReadManyList.Count);

                // Test ReadMany with predicate - simple equality
                IEnumerable<ComplexEntity> entitiesWithHighPrice = repository.ReadMany(e => e.Price > 200);
                var highPriceList = entitiesWithHighPrice.ToList();
                Assert.True(highPriceList.Count >= 2);
                Assert.All(highPriceList, e => Assert.True(e.Price > 200));

                // Test ReadMany with predicate - complex condition
                IEnumerable<ComplexEntity> entitiesInRange = repository.ReadMany(e => e.Price >= 50 && e.Price <= 150);
                var inRangeList = entitiesInRange.ToList();
                Assert.True(inRangeList.Count >= 1);
                Assert.All(inRangeList, e =>
                {
                    Assert.True(e.Price >= 50);
                    Assert.True(e.Price <= 150);
                });

                // Test ReadMany with predicate - string operations
                IEnumerable<ComplexEntity> entitiesWithNameFilter = repository.ReadMany(e => e.Name.StartsWith("Entity"));
                var nameFilterList = entitiesWithNameFilter.ToList();
                Assert.True(nameFilterList.Count >= 5);
                Assert.All(nameFilterList, e => Assert.StartsWith("Entity", e.Name));

                // Test async versions
                var allEntitiesAsync = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadAllAsync())
                {
                    allEntitiesAsync.Add(entity);
                }
                Assert.Equal(allEntitiesList.Count, allEntitiesAsync.Count);

                // Test ReadManyAsync with predicate
                var highPriceAsyncList = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadManyAsync(e => e.Price > 200))
                {
                    highPriceAsyncList.Add(entity);
                }
                Assert.Equal(highPriceList.Count, highPriceAsyncList.Count);

                // Test ReadManyAsync with null predicate (should be same as ReadAllAsync)
                var allFromReadManyAsyncList = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadManyAsync(null))
                {
                    allFromReadManyAsyncList.Add(entity);
                }
                Assert.Equal(allEntitiesAsync.Count, allFromReadManyAsyncList.Count);

                // Test with nullable field filters - debug version
                IEnumerable<ComplexEntity> entitiesWithNullableInt = repository.ReadMany(e => e.NullableInt != null);
                var withNullableList = entitiesWithNullableInt.ToList();
                Assert.True(withNullableList.Count >= 4, $"Expected at least 4 entities with non-null nullable_int, got {withNullableList.Count}"); // All entities except Entity4 which has null
                Assert.All(withNullableList, e => Assert.True(e.NullableInt.HasValue, $"Entity {e.Name} should have a non-null nullable_int but doesn't"));

                // Test equality filter
                IEnumerable<ComplexEntity> specificEntity = repository.ReadMany(e => e.Name == "Entity1");
                var specificList = specificEntity.ToList();
                Assert.Single(specificList);
                Assert.Equal("Entity1", specificList[0].Name);
            }
            finally
            {
                // Cleanup
                await DropTestTableAsync(connectionFactory);
            }
        }

        /// <summary>
        /// Tests PostgreSQL collection operations with transaction support.
        /// </summary>
        [Fact]
        public async Task PostgresCollectionOperationsWorkWithTransactions()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            await CreateTestTableAsync(connectionFactory);

            try
            {
                // Insert test data first
                await InsertTestDataAsync(connectionFactory);

                using var transaction = await repository.BeginTransactionAsync();

                // Test collection operations within transaction
                IEnumerable<ComplexEntity> allEntities = repository.ReadAll(transaction);
                Assert.True(allEntities.Count() >= 5);

                IEnumerable<ComplexEntity> filteredEntities = repository.ReadMany(e => e.Price > 100, transaction);
                Assert.True(filteredEntities.Count() >= 2);

                // Test async versions within transaction
                var asyncEntities = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadAllAsync(transaction))
                {
                    asyncEntities.Add(entity);
                }
                Assert.Equal(allEntities.Count(), asyncEntities.Count);

                var filteredAsyncEntities = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadManyAsync(e => e.Price > 100, transaction))
                {
                    filteredAsyncEntities.Add(entity);
                }
                Assert.Equal(filteredEntities.Count(), filteredAsyncEntities.Count);

                await transaction.CommitAsync();
            }
            finally
            {
                await DropTestTableAsync(connectionFactory);
            }
        }

        /// <summary>
        /// Tests error handling and edge cases for collection operations.
        /// </summary>
        [Fact]
        public async Task PostgresCollectionOperationsHandleEdgeCases()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            await CreateTestTableAsync(connectionFactory);

            try
            {
                // Test empty table results
                IEnumerable<ComplexEntity> emptyResults = repository.ReadAll();
                Assert.Empty(emptyResults);

                IEnumerable<ComplexEntity> emptyFilteredResults = repository.ReadMany(e => e.Price > 0);
                Assert.Empty(emptyFilteredResults);

                // Test async versions with empty results
                var emptyAsyncResults = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadAllAsync())
                {
                    emptyAsyncResults.Add(entity);
                }
                Assert.Empty(emptyAsyncResults);

                var emptyFilteredAsyncResults = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadManyAsync(e => e.Price > 0))
                {
                    emptyFilteredAsyncResults.Add(entity);
                }
                Assert.Empty(emptyFilteredAsyncResults);

                // Insert some data and test filters that return no results
                await InsertTestDataAsync(connectionFactory);

                IEnumerable<ComplexEntity> noMatchResults = repository.ReadMany(e => e.Price > 10000);
                Assert.Empty(noMatchResults);

                var noMatchAsyncResults = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadManyAsync(e => e.Price > 10000))
                {
                    noMatchAsyncResults.Add(entity);
                }
                Assert.Empty(noMatchAsyncResults);
            }
            finally
            {
                await DropTestTableAsync(connectionFactory);
            }
        }

        /// <summary>
        /// Tests cancellation token support in async collection operations.
        /// </summary>
        [Fact]
        public async Task PostgresCollectionOperationsSupportCancellation()
        {
            if (_SkipTests) return;

            using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
            using var repository = new PostgresRepository<ComplexEntity>(connectionFactory);

            await CreateTestTableAsync(connectionFactory);

            try
            {
                await InsertTestDataAsync(connectionFactory);

                using var cts = new CancellationTokenSource();

                // Test successful operations with valid token
                var validResults = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadAllAsync(token: cts.Token))
                {
                    validResults.Add(entity);
                }
                Assert.True(validResults.Count >= 5);

                // Test ReadManyAsync with valid token
                var validFilteredResults = new List<ComplexEntity>();
                await foreach (ComplexEntity entity in repository.ReadManyAsync(e => e.Price > 100, token: cts.Token))
                {
                    validFilteredResults.Add(entity);
                }
                Assert.True(validFilteredResults.Count >= 2);

                // Test with pre-cancelled token
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                {
                    await foreach (ComplexEntity _ in repository.ReadAllAsync(token: cts.Token))
                    {
                        // Should not reach here
                    }
                });
            }
            finally
            {
                await DropTestTableAsync(connectionFactory);
            }
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Sets up the test database and handles connection failures.
        /// </summary>
        private void SetupTestDatabase()
        {
            try
            {
                using var connectionFactory = new PostgresConnectionFactory(TestConnectionString);
                using var connection = connectionFactory.GetConnection();

                // Ensure connection is open (GetConnection might return an already open connection)
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                }

                // Test the connection with a simple query
                using var command = new NpgsqlCommand("SELECT 1", (NpgsqlConnection)connection);
                command.ExecuteScalar();

                // If we get here, the connection works
                _SkipTests = false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Skipping PostgreSQL integration tests due to connection failure: {ex.Message}");
                Console.WriteLine("To run PostgreSQL tests, ensure PostgreSQL is running and the test database is configured.");
                _SkipTests = true;
            }
        }

        /// <summary>
        /// Creates the test table for ComplexEntity.
        /// </summary>
        private async Task CreateTestTableAsync(PostgresConnectionFactory connectionFactory)
        {
            using var connection = await connectionFactory.GetConnectionAsync();

            // Ensure connection is open (GetConnectionAsync might return an already open connection)
            if (connection.State != System.Data.ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            string createTableSql = @"
                CREATE TABLE IF NOT EXISTS complex_entities (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMPTZ,
                    unique_id UUID DEFAULT gen_random_uuid(),
                    duration INTERVAL,
                    status INTEGER,
                    status_int INTEGER,
                    tags TEXT[],
                    scores INTEGER[],
                    metadata JSONB,
                    address JSONB,
                    is_active BOOLEAN DEFAULT true,
                    nullable_int INTEGER,
                    price DECIMAL(10,2)
                );";

            using var command = new NpgsqlCommand(createTableSql, (NpgsqlConnection)connection);
            await command.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Inserts test data using raw SQL to avoid entity mapping issues.
        /// </summary>
        private async Task InsertTestDataAsync(PostgresConnectionFactory connectionFactory)
        {
            using var connection = await connectionFactory.GetConnectionAsync();

            if (connection.State != System.Data.ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            string insertSql = @"
                INSERT INTO complex_entities (name, price, created_date, unique_id, nullable_int) VALUES
                ('Entity1', 100.50, NOW(), gen_random_uuid(), 10),
                ('Entity2', 250.75, NOW(), gen_random_uuid(), 20),
                ('Entity3', 75.25, NOW(), gen_random_uuid(), 15),
                ('Entity4', 300.00, NOW(), gen_random_uuid(), null),
                ('Entity5', 50.00, NOW(), gen_random_uuid(), 5);";

            using var command = new NpgsqlCommand(insertSql, (NpgsqlConnection)connection);
            await command.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Drops the test table for ComplexEntity.
        /// </summary>
        private async Task DropTestTableAsync(PostgresConnectionFactory connectionFactory)
        {
            try
            {
                using var connection = await connectionFactory.GetConnectionAsync();

                // Ensure connection is open (GetConnectionAsync might return an already open connection)
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                string dropTableSql = "DROP TABLE IF EXISTS complex_entities;";
                using var command = new NpgsqlCommand(dropTableSql, (NpgsqlConnection)connection);
                await command.ExecuteNonQueryAsync();
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

        #endregion

        #region IDisposable

        /// <summary>
        /// Disposes of test resources.
        /// </summary>
        public void Dispose()
        {
            // Cleanup if needed
        }

        #endregion
    }
}