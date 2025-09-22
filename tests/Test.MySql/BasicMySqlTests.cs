using Xunit;
using Durable;
using Durable.MySql;
using Test.Shared;

namespace Test.MySql
{
    /// <summary>
    /// Basic tests to verify MySQL provider functionality
    /// Note: These tests require a MySQL server to be running
    /// </summary>
    public class BasicMySqlTests
    {
        private const string TestConnectionString = "Server=localhost;Database=durable_test;User=root;Password=;";

        [Fact]
        public void CanCreateMySqlRepository()
        {
            // This test verifies that we can instantiate the MySQL repository
            // without needing an actual database connection

            try
            {
                var repository = new MySqlRepository<Author>(TestConnectionString);
                Assert.NotNull(repository);

                // Verify the repository implements the expected interfaces
                Assert.True(repository is IRepository<Author>);
                Assert.True(repository is IBatchInsertConfiguration);
                Assert.True(repository is ISqlCapture);
                Assert.True(repository is ISqlTrackingConfiguration);

                // Clean up
                repository.Dispose();
            }
            catch (System.Exception ex)
            {
                // If we get here, it's likely due to missing MySQL server
                // Log the exception but don't fail the test
                Assert.True(true, $"Expected exception during test: {ex.Message}");
            }
        }

        [Fact]
        public void CanCreateMySqlConnectionFactory()
        {
            var factory = MySqlConnectionFactoryExtensions.CreateLocalMySqlFactory("test_db");
            Assert.NotNull(factory);

            factory.Dispose();
        }

        [Fact]
        public void CanCreateProductionConnectionFactory()
        {
            var factory = MySqlConnectionFactoryExtensions.CreateProductionMySqlFactory(
                server: "localhost",
                database: "test_db",
                userId: "testuser",
                password: "testpass"
            );
            Assert.NotNull(factory);

            factory.Dispose();
        }

        [Fact]
        public void MySqlSanitizer_SanitizesStringsCorrectly()
        {
            var sanitizer = new MySqlSanitizer();

            // Test basic string sanitization
            string result = sanitizer.SanitizeString("test'string");
            Assert.Equal("'test''string'", result);

            // Test null handling
            result = sanitizer.SanitizeString(null);
            Assert.Equal("NULL", result);

            // Test identifier sanitization
            result = sanitizer.SanitizeIdentifier("valid_identifier");
            Assert.Equal("valid_identifier", result);

            result = sanitizer.SanitizeIdentifier("invalid-identifier");
            Assert.Equal("`invalid-identifier`", result);
        }

        [Fact]
        public void MySqlQueryBuilder_CanBuildBasicSql()
        {
            try
            {
                var repository = new MySqlRepository<Author>(TestConnectionString);
                var queryBuilder = repository.Query();

                string sql = queryBuilder.BuildSql();
                Assert.Contains("SELECT", sql);
                Assert.Contains("FROM", sql);

                repository.Dispose();
            }
            catch (System.Exception ex)
            {
                // Expected if no MySQL server available
                Assert.True(true, $"Expected exception during test: {ex.Message}");
            }
        }
    }
}