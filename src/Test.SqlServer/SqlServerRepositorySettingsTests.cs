namespace Test.SqlServer
{
    using System;
    using Durable;
    using Durable.SqlServer;
    using Microsoft.Data.SqlClient;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Tests for SqlServerRepositorySettings functionality
    /// </summary>
    public class SqlServerRepositorySettingsTests
    {
        [Fact]
        public void Parse_ValidConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=testdb;User Id=sa;Password=secret;Encrypt=True;TrustServerCertificate=True;";

            // Act
            SqlServerRepositorySettings settings = SqlServerRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal("localhost", settings.Hostname);
            Assert.Null(settings.Port); // No port specified
            Assert.Equal("testdb", settings.Database);
            Assert.Equal("sa", settings.Username);
            Assert.Equal("secret", settings.Password);
            Assert.True(settings.Encrypt);
            Assert.True(settings.TrustServerCertificate);
        }

        [Fact]
        public void Parse_WithPort_ShouldParsePort()
        {
            // Arrange
            string connectionString = "Server=localhost,1434;Database=testdb;User Id=sa;Password=secret;";

            // Act
            SqlServerRepositorySettings settings = SqlServerRepositorySettings.Parse(connectionString);

            // Assert
            Assert.Equal("localhost", settings.Hostname);
            Assert.Equal(1434, settings.Port);
        }

        [Fact]
        public void Parse_MinimalConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=testdb;Integrated Security=True;";

            // Act
            SqlServerRepositorySettings settings = SqlServerRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal("localhost", settings.Hostname);
            Assert.Equal("testdb", settings.Database);
            Assert.True(settings.IntegratedSecurity);
            Assert.Null(settings.Username);
            Assert.Null(settings.Password);
        }

        [Fact]
        public void Parse_NullConnectionString_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => SqlServerRepositorySettings.Parse(null));
        }

        [Fact]
        public void Parse_EmptyConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlServerRepositorySettings.Parse(""));
        }

        [Fact]
        public void BuildConnectionString_ValidSettings_ShouldBuildCorrectly()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Port = 1434,
                Database = "testdb",
                Username = "sa",
                Password = "secret",
                Encrypt = true,
                TrustServerCertificate = true,
                MaxPoolSize = 50
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Data Source=localhost,1434", connectionString);
            Assert.Contains("Initial Catalog=testdb", connectionString);
            Assert.Contains("User ID=sa", connectionString);
            Assert.Contains("Password=secret", connectionString);
            Assert.Contains("Encrypt=True", connectionString);
            Assert.Contains("Trust Server Certificate=True", connectionString);
            Assert.Contains("Max Pool Size=50", connectionString);
        }

        [Fact]
        public void BuildConnectionString_WithoutPort_ShouldNotIncludePortInDataSource()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Data Source=localhost", connectionString);
            Assert.DoesNotContain(",", connectionString.Split(';')[0]); // First part shouldn't have comma
        }

        [Fact]
        public void BuildConnectionString_MissingHostname_ShouldThrowInvalidOperationException()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Database = "testdb"
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => settings.BuildConnectionString());
        }

        [Fact]
        public void BuildConnectionString_MissingDatabase_ShouldThrowInvalidOperationException()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost"
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => settings.BuildConnectionString());
        }

        [Fact]
        public void RepositoryConstructor_WithSettings_ShouldSetSettingsProperty()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act
            SqlServerRepository<Author> repository = new SqlServerRepository<Author>(settings);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal(settings, repository.Settings);
            Assert.Equal(RepositoryType.SqlServer, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryConstructor_WithConnectionString_ShouldParseAndSetSettings()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=testdb;User Id=sa;Password=secret;";

            // Act
            SqlServerRepository<Author> repository = new SqlServerRepository<Author>(connectionString);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal("localhost", repository.Settings.Hostname);
            Assert.Equal("testdb", repository.Settings.Database);
            Assert.Equal(RepositoryType.SqlServer, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryType_SqlServer_ShouldHaveCorrectProperties()
        {
            // Act & Assert
            Assert.Equal("sqlserver", RepositoryType.SqlServer.Identifier);
            Assert.Equal("SQL Server", RepositoryType.SqlServer.DisplayName);
        }

        [Fact]
        public void ParseAndBuild_RoundTrip_ShouldPreserveSettings()
        {
            // Arrange
            string originalConnectionString = "Server=localhost,1434;Database=testdb;User Id=sa;Password=secret;Encrypt=True;Trust Server Certificate=True;Max Pool Size=100;";

            // Act
            SqlServerRepositorySettings settings = SqlServerRepositorySettings.Parse(originalConnectionString);
            string rebuiltConnectionString = settings.BuildConnectionString();
            SqlServerRepositorySettings reparsedSettings = SqlServerRepositorySettings.Parse(rebuiltConnectionString);

            // Assert
            Assert.Equal(settings.Hostname, reparsedSettings.Hostname);
            Assert.Equal(settings.Port, reparsedSettings.Port);
            Assert.Equal(settings.Database, reparsedSettings.Database);
            Assert.Equal(settings.Username, reparsedSettings.Username);
            Assert.Equal(settings.Password, reparsedSettings.Password);
            Assert.Equal(settings.Encrypt, reparsedSettings.Encrypt);
            Assert.Equal(settings.TrustServerCertificate, reparsedSettings.TrustServerCertificate);
            Assert.Equal(settings.MaxPoolSize, reparsedSettings.MaxPoolSize);
        }

        [Fact]
        public void Settings_WithPoolingOptions_ShouldPreserveThem()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb",
                MinPoolSize = 5,
                MaxPoolSize = 50,
                Pooling = true
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Min Pool Size=5", connectionString);
            Assert.Contains("Max Pool Size=50", connectionString);
            Assert.Contains("Pooling=True", connectionString);
        }

        [Fact]
        public void Settings_WithIntegratedSecurity_ShouldNotIncludeCredentials()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb",
                IntegratedSecurity = true
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Integrated Security=True", connectionString);
            Assert.DoesNotContain("User ID", connectionString);
            Assert.DoesNotContain("Password", connectionString);
        }

        [Fact]
        public void Settings_TypeProperty_ShouldReturnSqlServer()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act & Assert
            Assert.Equal(RepositoryType.SqlServer, settings.Type);
            Assert.Equal("sqlserver", settings.Type.Identifier);
            Assert.Equal("SQL Server", settings.Type.DisplayName);
        }

        [Fact]
        public void Settings_WithConnectionTimeout_ShouldPreserveThem()
        {
            // Arrange
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb",
                ConnectionTimeout = 30
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Connect Timeout=30", connectionString);
        }
    }
}
