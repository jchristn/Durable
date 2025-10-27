namespace Test.MySql
{
    using System;
    using Durable;
    using Durable.MySql;
    using MySqlConnector;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Tests for MySqlRepositorySettings functionality
    /// </summary>
    public class MySqlRepositorySettingsTests
    {
        [Fact]
        public void Parse_ValidConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Server=localhost;Port=3306;Database=testdb;User=root;Password=secret;SslMode=Required;";

            // Act
            MySqlRepositorySettings settings = MySqlRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal("localhost", settings.Hostname);
            Assert.Null(settings.Port); // 3306 is default, so should be null
            Assert.Equal("testdb", settings.Database);
            Assert.Equal("root", settings.Username);
            Assert.Equal("secret", settings.Password);
            Assert.Equal(MySqlSslMode.Required, settings.SslMode);
        }

        [Fact]
        public void Parse_NonDefaultPort_ShouldParsePort()
        {
            // Arrange
            string connectionString = "Server=localhost;Port=3307;Database=testdb;User=root;Password=secret;";

            // Act
            MySqlRepositorySettings settings = MySqlRepositorySettings.Parse(connectionString);

            // Assert
            Assert.Equal(3307, settings.Port);
        }

        [Fact]
        public void Parse_MinimalConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=testdb;";

            // Act
            MySqlRepositorySettings settings = MySqlRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal("localhost", settings.Hostname);
            Assert.Equal("testdb", settings.Database);
            Assert.Null(settings.Username);
            Assert.Null(settings.Password);
        }

        [Fact]
        public void Parse_NullConnectionString_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => MySqlRepositorySettings.Parse(null));
        }

        [Fact]
        public void Parse_EmptyConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => MySqlRepositorySettings.Parse(""));
        }

        [Fact]
        public void BuildConnectionString_ValidSettings_ShouldBuildCorrectly()
        {
            // Arrange
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Hostname = "localhost",
                Port = 3307,
                Database = "testdb",
                Username = "root",
                Password = "secret",
                SslMode = MySqlSslMode.Required,
                MaximumPoolSize = 50
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Server=localhost", connectionString);
            Assert.Contains("Port=3307", connectionString);
            Assert.Contains("Database=testdb", connectionString);
            Assert.Contains("User ID=root", connectionString);
            Assert.Contains("Password=secret", connectionString);
            Assert.Contains("SSL Mode=Required", connectionString);
            Assert.Contains("Maximum Pool Size=50", connectionString);
        }

        [Fact]
        public void BuildConnectionString_MissingHostname_ShouldThrowInvalidOperationException()
        {
            // Arrange
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Database = "testdb"
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => settings.BuildConnectionString());
        }

        [Fact]
        public void BuildConnectionString_MissingDatabase_ShouldSucceed()
        {
            // Arrange
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Hostname = "localhost"
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert - Database is now optional to support CreateDatabaseIfNotExists
            Assert.NotNull(connectionString);
            Assert.Contains("Server=localhost", connectionString);
        }

        [Fact]
        public void RepositoryConstructor_WithSettings_ShouldSetSettingsProperty()
        {
            // Arrange
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act
            MySqlRepository<Author> repository = new MySqlRepository<Author>(settings);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal(settings, repository.Settings);
            Assert.Equal(RepositoryType.MySql, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryConstructor_WithConnectionString_ShouldParseAndSetSettings()
        {
            // Arrange
            string connectionString = "Server=localhost;Database=testdb;User=root;Password=secret;";

            // Act
            MySqlRepository<Author> repository = new MySqlRepository<Author>(connectionString);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal("localhost", repository.Settings.Hostname);
            Assert.Equal("testdb", repository.Settings.Database);
            Assert.Equal(RepositoryType.MySql, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryType_MySql_ShouldHaveCorrectProperties()
        {
            // Act & Assert
            Assert.Equal("mysql", RepositoryType.MySql.Identifier);
            Assert.Equal("MySQL", RepositoryType.MySql.DisplayName);
        }

        [Fact]
        public void ParseAndBuild_RoundTrip_ShouldPreserveSettings()
        {
            // Arrange
            string originalConnectionString = "Server=localhost;Port=3307;Database=testdb;User=root;Password=secret;SSL Mode=Required;Maximum Pool Size=100;";

            // Act
            MySqlRepositorySettings settings = MySqlRepositorySettings.Parse(originalConnectionString);
            string rebuiltConnectionString = settings.BuildConnectionString();
            MySqlRepositorySettings reparsedSettings = MySqlRepositorySettings.Parse(rebuiltConnectionString);

            // Assert
            Assert.Equal(settings.Hostname, reparsedSettings.Hostname);
            Assert.Equal(settings.Port, reparsedSettings.Port);
            Assert.Equal(settings.Database, reparsedSettings.Database);
            Assert.Equal(settings.Username, reparsedSettings.Username);
            Assert.Equal(settings.Password, reparsedSettings.Password);
            Assert.Equal(settings.SslMode, reparsedSettings.SslMode);
            Assert.Equal(settings.MaximumPoolSize, reparsedSettings.MaximumPoolSize);
        }

        [Fact]
        public void Settings_WithPoolingOptions_ShouldPreserveThem()
        {
            // Arrange
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb",
                MinimumPoolSize = 5,
                MaximumPoolSize = 50,
                Pooling = true
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Minimum Pool Size=5", connectionString);
            Assert.Contains("Maximum Pool Size=50", connectionString);
            Assert.Contains("Pooling=True", connectionString);
        }

        [Fact]
        public void Settings_TypeProperty_ShouldReturnMySql()
        {
            // Arrange
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act & Assert
            Assert.Equal(RepositoryType.MySql, settings.Type);
            Assert.Equal("mysql", settings.Type.Identifier);
            Assert.Equal("MySQL", settings.Type.DisplayName);
        }
    }
}
