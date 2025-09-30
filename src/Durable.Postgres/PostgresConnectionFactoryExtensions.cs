namespace Durable.Postgres
{
    using System;
    using Npgsql;

    /// <summary>
    /// Extension methods for PostgreSQL connection factory to provide convenient setup options.
    /// </summary>
    public static class PostgresConnectionFactoryExtensions
    {
        /// <summary>
        /// Creates a PostgreSQL connection factory with common connection string parameters.
        /// </summary>
        /// <param name="host">The PostgreSQL server hostname or IP address</param>
        /// <param name="database">The database name</param>
        /// <param name="username">The username for authentication</param>
        /// <param name="password">The password for authentication</param>
        /// <param name="port">The port number (default is 5432)</param>
        /// <param name="pooling">Whether to enable connection pooling (default is true)</param>
        /// <param name="minPoolSize">Minimum pool size (default is 0)</param>
        /// <param name="maxPoolSize">Maximum pool size (default is 100)</param>
        /// <param name="connectionTimeout">Connection timeout in seconds (default is 30)</param>
        /// <param name="commandTimeout">Command timeout in seconds (default is 30)</param>
        /// <param name="sslMode">SSL mode for secure connections (default is Prefer)</param>
        /// <returns>A configured PostgreSQL connection factory</returns>
        public static PostgresConnectionFactory CreatePostgresFactory(
            string host,
            string database,
            string username,
            string password,
            int port = 5432,
            bool pooling = true,
            int minPoolSize = 0,
            int maxPoolSize = 100,
            int connectionTimeout = 30,
            int commandTimeout = 30,
            string sslMode = "Prefer")
        {
            if (string.IsNullOrWhiteSpace(host))
                throw new ArgumentException("Host cannot be null or empty", nameof(host));
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Database cannot be null or empty", nameof(database));
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username cannot be null or empty", nameof(username));

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
            {
                Host = host,
                Database = database,
                Username = username,
                Password = password,
                Port = port,
                Pooling = pooling,
                MinPoolSize = minPoolSize,
                MaxPoolSize = maxPoolSize,
                Timeout = connectionTimeout,
                CommandTimeout = commandTimeout,
                SslMode = Enum.Parse<SslMode>(sslMode, ignoreCase: true)
            };

            ConnectionPoolOptions? poolOptions = pooling ? new ConnectionPoolOptions
            {
                MinPoolSize = minPoolSize,
                MaxPoolSize = maxPoolSize,
                ConnectionTimeout = TimeSpan.FromSeconds(connectionTimeout)
            } : null;

            return new PostgresConnectionFactory(builder.ConnectionString, poolOptions);
        }

        /// <summary>
        /// Creates a PostgreSQL connection factory for local development with default settings.
        /// </summary>
        /// <param name="database">The database name</param>
        /// <param name="username">The username (default is "postgres")</param>
        /// <param name="password">The password (default is empty)</param>
        /// <param name="host">The host (default is "localhost")</param>
        /// <param name="port">The port (default is 5432)</param>
        /// <returns>A configured PostgreSQL connection factory for local development</returns>
        public static PostgresConnectionFactory CreateLocalPostgresFactory(
            string database,
            string username = "postgres",
            string password = "",
            string host = "localhost",
            int port = 5432)
        {
            return CreatePostgresFactory(
                host: host,
                database: database,
                username: username,
                password: password,
                port: port,
                pooling: true,
                minPoolSize: 1,
                maxPoolSize: 10,
                sslMode: "Disable"
            );
        }

        /// <summary>
        /// Creates a PostgreSQL connection factory configured for production use with recommended settings.
        /// </summary>
        /// <param name="host">The PostgreSQL server hostname</param>
        /// <param name="database">The database name</param>
        /// <param name="username">The username for authentication</param>
        /// <param name="password">The password for authentication</param>
        /// <param name="port">The port number (default is 5432)</param>
        /// <returns>A configured PostgreSQL connection factory optimized for production</returns>
        public static PostgresConnectionFactory CreateProductionPostgresFactory(
            string host,
            string database,
            string username,
            string password,
            int port = 5432)
        {
            return CreatePostgresFactory(
                host: host,
                database: database,
                username: username,
                password: password,
                port: port,
                pooling: true,
                minPoolSize: 5,
                maxPoolSize: 50,
                connectionTimeout: 15,
                commandTimeout: 60,
                sslMode: "Require"
            );
        }

        /// <summary>
        /// Creates a PostgreSQL connection factory with Unix domain socket connection for local development.
        /// </summary>
        /// <param name="database">The database name</param>
        /// <param name="username">The username (default is "postgres")</param>
        /// <param name="password">The password (default is empty)</param>
        /// <param name="socketDirectory">The Unix socket directory (default is "/tmp")</param>
        /// <returns>A configured PostgreSQL connection factory using Unix domain socket</returns>
        public static PostgresConnectionFactory CreateUnixSocketPostgresFactory(
            string database,
            string username = "postgres",
            string password = "",
            string socketDirectory = "/tmp")
        {
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Database cannot be null or empty", nameof(database));
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username cannot be null or empty", nameof(username));

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
            {
                Host = socketDirectory,
                Database = database,
                Username = username,
                Password = password,
                Pooling = true,
                MinPoolSize = 1,
                MaxPoolSize = 10,
                Timeout = 30,
                CommandTimeout = 30,
                SslMode = SslMode.Disable
            };

            ConnectionPoolOptions poolOptions = new ConnectionPoolOptions
            {
                MinPoolSize = 1,
                MaxPoolSize = 10,
                ConnectionTimeout = TimeSpan.FromSeconds(30)
            };

            return new PostgresConnectionFactory(builder.ConnectionString, poolOptions);
        }
    }
}