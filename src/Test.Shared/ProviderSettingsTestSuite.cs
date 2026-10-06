namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using Durable.MySql;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Durable.SqlServer;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.Sqlite;
    using MySqlConnector;
    using Npgsql;
    using Xunit;

    /// <summary>
    /// The four providers' repository settings share one shape for pool and timeout settings (ConnectionTimeout,
    /// MinPoolSize, MaxPoolSize, Pooling as int?/bool?) and map them, plus the TLS settings, to the driver's connection
    /// string and back. The connection factories accept the same settings. No database is needed.
    /// </summary>
    public class ProviderSettingsTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// MySQL pool, timeout and TLS settings reach the MySqlConnector connection string and parse back.
        /// </summary>
        [Fact]
        public void MySqlSettingsMapToConnectionString()
        {
            MySqlRepositorySettings settings = new MySqlRepositorySettings
            {
                Hostname = "db.example",
                Port = 3307,
                Username = "app",
                Password = "secret",
                Database = "shop",
                ConnectionTimeout = 21,
                MinPoolSize = 2,
                MaxPoolSize = 40,
                Pooling = false,
                SslMode = MySqlSslMode.Required
            };

            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder(settings.BuildConnectionString());
            Assert.Equal("db.example", builder.Server);
            Assert.Equal(3307u, builder.Port);
            Assert.Equal(21u, builder.ConnectionTimeout);
            Assert.Equal(2u, builder.MinimumPoolSize);
            Assert.Equal(40u, builder.MaximumPoolSize);
            Assert.False(builder.Pooling);
            Assert.Equal(MySqlSslMode.Required, builder.SslMode);

            MySqlRepositorySettings parsed = MySqlRepositorySettings.Parse(settings.BuildConnectionString());
            Assert.Equal(21, parsed.ConnectionTimeout);
            Assert.Equal(2, parsed.MinPoolSize);
            Assert.Equal(40, parsed.MaxPoolSize);
            Assert.False(parsed.Pooling);
            Assert.Equal(MySqlSslMode.Required, parsed.SslMode);
            Assert.Equal(3307, parsed.Port);

            MySqlRepositorySettings defaults = MySqlRepositorySettings.Parse("Server=h;Database=d");
            Assert.Null(defaults.ConnectionTimeout);
            Assert.Null(defaults.MinPoolSize);
            Assert.Null(defaults.MaxPoolSize);
            Assert.Null(defaults.Pooling);
            Assert.Null(defaults.SslMode);

            Assert.Throws<OverflowException>(() => new MySqlRepositorySettings { Hostname = "h", MaxPoolSize = -1 }.BuildConnectionString());

            using MySqlConnectionFactory factory = new MySqlConnectionFactory(settings);
            Assert.Equal(settings.BuildConnectionString(), factory.ConnectionString);
        }

        /// <summary>
        /// PostgreSQL pool, timeout and TLS settings reach the Npgsql connection string and parse back; a command timeout
        /// in the connection string is preserved as an additional property.
        /// </summary>
        [Fact]
        public void PostgresSettingsMapToConnectionString()
        {
            PostgresRepositorySettings settings = new PostgresRepositorySettings
            {
                Hostname = "pg.example",
                Port = 5433,
                Username = "app",
                Password = "secret",
                Database = "shop",
                ConnectionTimeout = 22,
                MinPoolSize = 3,
                MaxPoolSize = 30,
                Pooling = false,
                SslMode = SslMode.Require
            };

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder(settings.BuildConnectionString());
            Assert.Equal("pg.example", builder.Host);
            Assert.Equal(5433, builder.Port);
            Assert.Equal(22, builder.Timeout);
            Assert.Equal(3, builder.MinPoolSize);
            Assert.Equal(30, builder.MaxPoolSize);
            Assert.False(builder.Pooling);
            Assert.Equal(SslMode.Require, builder.SslMode);

            PostgresRepositorySettings parsed = PostgresRepositorySettings.Parse(settings.BuildConnectionString() + ";Command Timeout=45");
            Assert.Equal(22, parsed.ConnectionTimeout);
            Assert.Equal(3, parsed.MinPoolSize);
            Assert.Equal(30, parsed.MaxPoolSize);
            Assert.False(parsed.Pooling);
            Assert.Equal(SslMode.Require, parsed.SslMode);
            Assert.NotNull(parsed.AdditionalProperties);
            Assert.Equal(45, new NpgsqlConnectionStringBuilder(parsed.BuildConnectionString()).CommandTimeout);

            PostgresRepositorySettings defaults = PostgresRepositorySettings.Parse("Host=h;Database=d");
            Assert.Null(defaults.ConnectionTimeout);
            Assert.Null(defaults.MinPoolSize);
            Assert.Null(defaults.MaxPoolSize);
            Assert.Null(defaults.Pooling);
            Assert.Null(defaults.SslMode);

            using PostgresConnectionFactory factory = new PostgresConnectionFactory(settings);
            Assert.Equal(settings.BuildConnectionString(), factory.ConnectionString);
        }

        /// <summary>
        /// SQL Server pool, timeout and TLS settings reach the SqlClient connection string and parse back.
        /// </summary>
        [Fact]
        public void SqlServerSettingsMapToConnectionString()
        {
            SqlServerRepositorySettings settings = new SqlServerRepositorySettings
            {
                Hostname = "sql.example",
                Port = 1434,
                Username = "app",
                Password = "secret",
                Database = "shop",
                ConnectionTimeout = 23,
                MinPoolSize = 4,
                MaxPoolSize = 20,
                Pooling = false,
                Encrypt = true,
                TrustServerCertificate = true
            };

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(settings.BuildConnectionString());
            Assert.Equal("sql.example,1434", builder.DataSource);
            Assert.Equal("shop", builder.InitialCatalog);
            Assert.Equal(23, builder.ConnectTimeout);
            Assert.Equal(4, builder.MinPoolSize);
            Assert.Equal(20, builder.MaxPoolSize);
            Assert.False(builder.Pooling);
            Assert.True(builder.TrustServerCertificate);

            SqlServerRepositorySettings parsed = SqlServerRepositorySettings.Parse(settings.BuildConnectionString());
            Assert.Equal("sql.example", parsed.Hostname);
            Assert.Equal(1434, parsed.Port);
            Assert.Equal(23, parsed.ConnectionTimeout);
            Assert.Equal(4, parsed.MinPoolSize);
            Assert.Equal(20, parsed.MaxPoolSize);
            Assert.False(parsed.Pooling);
            Assert.True(parsed.Encrypt);
            Assert.True(parsed.TrustServerCertificate);

            SqlServerRepositorySettings defaults = SqlServerRepositorySettings.Parse("Server=h;Database=d");
            Assert.Null(defaults.ConnectionTimeout);
            Assert.Null(defaults.MinPoolSize);
            Assert.Null(defaults.MaxPoolSize);
            Assert.Null(defaults.Pooling);

            using SqlServerConnectionFactory factory = new SqlServerConnectionFactory(settings);
            Assert.Equal(settings.BuildConnectionString(), factory.ConnectionString);
        }

        /// <summary>
        /// SQLite's Pooling setting reaches the Microsoft.Data.Sqlite connection string and parses back, and the SQLite
        /// factory accepts settings.
        /// </summary>
        [Fact]
        public void SqliteSettingsMapToConnectionString()
        {
            SqliteRepositorySettings settings = new SqliteRepositorySettings { DataSource = "settings-test.db", Pooling = false, Mode = SqliteOpenMode.ReadWrite };
            SqliteConnectionStringBuilder builder = new SqliteConnectionStringBuilder(settings.BuildConnectionString());
            Assert.False(builder.Pooling);
            Assert.Equal(SqliteOpenMode.ReadWrite, builder.Mode);

            SqliteRepositorySettings parsed = SqliteRepositorySettings.Parse(settings.BuildConnectionString());
            Assert.False(parsed.Pooling);
            Assert.Null(SqliteRepositorySettings.Parse("Data Source=x.db").Pooling);
            Assert.Equal(RepositoryType.Sqlite, parsed.Type);

            using SqliteConnectionFactory factory = new SqliteConnectionFactory(settings);
            Assert.Equal(settings.BuildConnectionString(), factory.ConnectionString);
            Assert.Throws<ArgumentNullException>(() => new SqliteConnectionFactory((SqliteRepositorySettings)null!));
        }

        /// <summary>
        /// Every provider's settings expose the same pool/timeout property names and types.
        /// </summary>
        [Fact]
        public void PoolAndTimeoutSettingsHaveOneShape()
        {
            Dictionary<string, Type> expected = new Dictionary<string, Type>
            {
                { "ConnectionTimeout", typeof(int?) },
                { "MinPoolSize", typeof(int?) },
                { "MaxPoolSize", typeof(int?) },
                { "Pooling", typeof(bool?) }
            };

            foreach (Type settingsType in new[] { typeof(MySqlRepositorySettings), typeof(PostgresRepositorySettings), typeof(SqlServerRepositorySettings) })
            {
                Assert.True(typeof(RepositorySettings).IsAssignableFrom(settingsType));
                foreach (KeyValuePair<string, Type> property in expected)
                {
                    System.Reflection.PropertyInfo? info = settingsType.GetProperty(property.Key);
                    Assert.True(info != null, settingsType.Name + " lacks " + property.Key);
                    Assert.Equal(property.Value, info!.PropertyType);
                }
            }

            Assert.Equal(typeof(bool?), typeof(SqliteRepositorySettings).GetProperty("Pooling")!.PropertyType);
        }

        #endregion
    }
}
