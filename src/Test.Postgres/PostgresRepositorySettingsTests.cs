namespace Test.Postgres
{
    using System;
    using Durable;
    using Durable.Postgres;
    using Npgsql;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Tests for PostgresRepositorySettings functionality
    /// </summary>
    public class PostgresRepositorySettingsTests
    {
        [Fact]
        public void Parse_ValidConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Host=localhost;Port=5432;Database=testdb;Username=postgres;Password=secret;SSL Mode=Require;";

            // Act
            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal("localhost", settings.Hostname);
            Assert.Null(settings.Port); // 5432 is default, so should be null
            Assert.Equal("testdb", settings.Database);
            Assert.Equal("postgres", settings.Username);
            Assert.Equal("secret", settings.Password);
            Assert.Equal(SslMode.Require, settings.SslMode);
        }

        [Fact]
        public void Parse_NonDefaultPort_ShouldParsePort()
        {
            // Arrange
            string connectionString = "Host=localhost;Port=5433;Database=testdb;Username=postgres;Password=secret;";

            // Act
            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connectionString);

            // Assert
            Assert.Equal(5433, settings.Port);
        }

        [Fact]
        public void Parse_MinimalConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Host=localhost;Database=testdb;";

            // Act
            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(connectionString);

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
            Assert.Throws<ArgumentNullException>(() => PostgresRepositorySettings.Parse(null));
        }

        [Fact]
        public void Parse_EmptyConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => PostgresRepositorySettings.Parse(""));
        }

        [Fact]
        public void BuildConnectionString_ValidSettings_ShouldBuildCorrectly()
        {
            // Arrange
            PostgresRepositorySettings settings = new PostgresRepositorySettings
            {
                Hostname = "localhost",
                Port = 5433,
                Database = "testdb",
                Username = "postgres",
                Password = "secret",
                SslMode = SslMode.Require,
                MaxPoolSize = 50,
                ConnectionTimeout = 30
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Host=localhost", connectionString);
            Assert.Contains("Port=5433", connectionString);
            Assert.Contains("Database=testdb", connectionString);
            Assert.Contains("Username=postgres", connectionString);
            Assert.Contains("Password=secret", connectionString);
            Assert.Contains("SSL Mode=Require", connectionString);
            Assert.Contains("Maximum Pool Size=50", connectionString);
            Assert.Contains("Timeout=30", connectionString);
        }

        [Fact]
        public void BuildConnectionString_MissingHostname_ShouldThrowInvalidOperationException()
        {
            // Arrange
            PostgresRepositorySettings settings = new PostgresRepositorySettings
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
            PostgresRepositorySettings settings = new PostgresRepositorySettings
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
            PostgresRepositorySettings settings = new PostgresRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act
            PostgresRepository<Author> repository = new PostgresRepository<Author>(settings);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal(settings, repository.Settings);
            Assert.Equal(RepositoryType.Postgres, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryConstructor_WithConnectionString_ShouldParseAndSetSettings()
        {
            // Arrange
            string connectionString = "Host=localhost;Database=testdb;Username=postgres;Password=secret;";

            // Act
            PostgresRepository<Author> repository = new PostgresRepository<Author>(connectionString);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal("localhost", repository.Settings.Hostname);
            Assert.Equal("testdb", repository.Settings.Database);
            Assert.Equal(RepositoryType.Postgres, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryType_Postgres_ShouldHaveCorrectProperties()
        {
            // Act & Assert
            Assert.Equal("postgres", RepositoryType.Postgres.Identifier);
            Assert.Equal("PostgreSQL", RepositoryType.Postgres.DisplayName);
        }

        [Fact]
        public void ParseAndBuild_RoundTrip_ShouldPreserveSettings()
        {
            // Arrange
            string originalConnectionString = "Host=localhost;Port=5433;Database=testdb;Username=postgres;Password=secret;SSL Mode=Require;Maximum Pool Size=100;";

            // Act
            PostgresRepositorySettings settings = PostgresRepositorySettings.Parse(originalConnectionString);
            string rebuiltConnectionString = settings.BuildConnectionString();
            PostgresRepositorySettings reparsedSettings = PostgresRepositorySettings.Parse(rebuiltConnectionString);

            // Assert
            Assert.Equal(settings.Hostname, reparsedSettings.Hostname);
            Assert.Equal(settings.Port, reparsedSettings.Port);
            Assert.Equal(settings.Database, reparsedSettings.Database);
            Assert.Equal(settings.Username, reparsedSettings.Username);
            Assert.Equal(settings.Password, reparsedSettings.Password);
            Assert.Equal(settings.SslMode, reparsedSettings.SslMode);
            Assert.Equal(settings.MaxPoolSize, reparsedSettings.MaxPoolSize);
        }

        [Fact]
        public void Settings_WithPoolingOptions_ShouldPreserveThem()
        {
            // Arrange
            PostgresRepositorySettings settings = new PostgresRepositorySettings
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
            Assert.Contains("Minimum Pool Size=5", connectionString);
            Assert.Contains("Maximum Pool Size=50", connectionString);
            Assert.Contains("Pooling=True", connectionString);
        }

        [Fact]
        public void Settings_WithTimeoutOptions_ShouldPreserveThem()
        {
            // Arrange
            PostgresRepositorySettings settings = new PostgresRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb",
                ConnectionTimeout = 20,
                CommandTimeout = 60
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Timeout=20", connectionString);
            Assert.Contains("Command Timeout=60", connectionString);
        }

        [Fact]
        public void Settings_TypeProperty_ShouldReturnPostgres()
        {
            // Arrange
            PostgresRepositorySettings settings = new PostgresRepositorySettings
            {
                Hostname = "localhost",
                Database = "testdb"
            };

            // Act & Assert
            Assert.Equal(RepositoryType.Postgres, settings.Type);
            Assert.Equal("postgres", settings.Type.Identifier);
            Assert.Equal("PostgreSQL", settings.Type.DisplayName);
        }
    }
}
