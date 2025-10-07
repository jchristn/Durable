#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Test.Sqlite
{
    using System;
    using Durable;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;
    using Xunit;

    /// <summary>
    /// Tests for SqliteRepositorySettings functionality
    /// </summary>
    public class RepositorySettingsTests
    {
        [Fact]
        public void Parse_ValidConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Data Source=test.db;Mode=ReadWrite;Cache=Shared";

            // Act
            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal("test.db", settings.DataSource);
            Assert.Equal(SqliteOpenMode.ReadWrite, settings.Mode);
            Assert.Equal(SqliteCacheMode.Shared, settings.CacheMode);
        }

        [Fact]
        public void Parse_MinimalConnectionString_ShouldParseCorrectly()
        {
            // Arrange
            string connectionString = "Data Source=:memory:";

            // Act
            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);

            // Assert
            Assert.NotNull(settings);
            Assert.Equal(":memory:", settings.DataSource);
            Assert.Null(settings.Mode);
            Assert.Null(settings.CacheMode);
        }

        [Fact]
        public void Parse_NullConnectionString_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => SqliteRepositorySettings.Parse(null));
        }

        [Fact]
        public void Parse_EmptyConnectionString_ShouldThrowArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqliteRepositorySettings.Parse(""));
        }

        [Fact]
        public void BuildConnectionString_ValidSettings_ShouldBuildCorrectly()
        {
            // Arrange
            SqliteRepositorySettings settings = new SqliteRepositorySettings
            {
                DataSource = "test.db",
                Mode = SqliteOpenMode.ReadWrite,
                CacheMode = SqliteCacheMode.Private
            };

            // Act
            string connectionString = settings.BuildConnectionString();

            // Assert
            Assert.Contains("Data Source=test.db", connectionString);
            Assert.Contains("Mode=ReadWrite", connectionString);
            Assert.Contains("Cache=Private", connectionString);
        }

        [Fact]
        public void BuildConnectionString_MissingDataSource_ShouldThrowInvalidOperationException()
        {
            // Arrange
            SqliteRepositorySettings settings = new SqliteRepositorySettings
            {
                Mode = SqliteOpenMode.ReadWrite
            };

            // Act & Assert
            Assert.Throws<InvalidOperationException>(() => settings.BuildConnectionString());
        }

        [Fact]
        public void RepositoryConstructor_WithSettings_ShouldSetSettingsProperty()
        {
            // Arrange
            SqliteRepositorySettings settings = new SqliteRepositorySettings
            {
                DataSource = ":memory:"
            };

            // Act
            SqliteRepository<Author> repository = new SqliteRepository<Author>(settings);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal(settings, repository.Settings);
            Assert.Equal(RepositoryType.Sqlite, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryConstructor_WithConnectionString_ShouldParseAndSetSettings()
        {
            // Arrange
            string connectionString = "Data Source=test.db;Mode=ReadWriteCreate";

            // Act
            SqliteRepository<Author> repository = new SqliteRepository<Author>(connectionString);

            // Assert
            Assert.NotNull(repository.Settings);
            Assert.Equal("test.db", ((SqliteRepositorySettings)repository.Settings).DataSource);
            Assert.Equal(RepositoryType.Sqlite, repository.Settings.Type);
        }

        [Fact]
        public void RepositoryType_Sqlite_ShouldHaveCorrectProperties()
        {
            // Act & Assert
            Assert.Equal("sqlite", RepositoryType.Sqlite.Identifier);
            Assert.Equal("SQLite", RepositoryType.Sqlite.DisplayName);
        }

        [Fact]
        public void RepositoryType_CustomType_ShouldBeCreatable()
        {
            // Act
            RepositoryType customType = new RepositoryType("custom", "Custom Database");

            // Assert
            Assert.Equal("custom", customType.Identifier);
            Assert.Equal("Custom Database", customType.DisplayName);
        }

        [Fact]
        public void RepositoryType_Equality_ShouldWorkCorrectly()
        {
            // Arrange
            RepositoryType type1 = new RepositoryType("test", "Test");
            RepositoryType type2 = new RepositoryType("test", "Different Display Name");
            RepositoryType type3 = new RepositoryType("different", "Test");

            // Assert
            Assert.True(type1 == type2); // Same identifier
            Assert.False(type1 == type3); // Different identifier
#pragma warning disable CS1718 // Comparison made to same variable
            Assert.True(RepositoryType.Sqlite == RepositoryType.Sqlite);
#pragma warning restore CS1718
        }

        [Fact]
        public void ParseAndBuild_RoundTrip_ShouldPreserveSettings()
        {
            // Arrange
            string originalConnectionString = "Data Source=test.db;Mode=ReadWrite;Cache=Shared";

            // Act
            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(originalConnectionString);
            string rebuiltConnectionString = settings.BuildConnectionString();
            SqliteRepositorySettings reparsedSettings = SqliteRepositorySettings.Parse(rebuiltConnectionString);

            // Assert
            Assert.Equal(settings.DataSource, reparsedSettings.DataSource);
            Assert.Equal(settings.Mode, reparsedSettings.Mode);
            Assert.Equal(settings.CacheMode, reparsedSettings.CacheMode);
        }

        [Fact]
        public void Settings_WithAdditionalProperties_ShouldPreserveThem()
        {
            // Arrange
            string connectionString = "Data Source=test.db;Foreign Keys=True";

            // Act
            SqliteRepositorySettings settings = SqliteRepositorySettings.Parse(connectionString);
            string rebuiltConnectionString = settings.BuildConnectionString();

            // Assert
            Assert.NotNull(settings.AdditionalProperties);
            Assert.Contains("Foreign Keys", rebuiltConnectionString);
        }
    }
}
