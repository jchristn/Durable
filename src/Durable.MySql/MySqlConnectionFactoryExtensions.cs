namespace Durable.MySql
{
    using System;

    /// <summary>
    /// Extension methods for MySQL connection factory to provide convenient setup options.
    /// </summary>
    public static class MySqlConnectionFactoryExtensions
    {
        /// <summary>
        /// Creates a MySQL connection factory with common connection string parameters.
        /// </summary>
        /// <param name="server">The MySQL server hostname or IP address</param>
        /// <param name="database">The database name</param>
        /// <param name="userId">The user ID for authentication</param>
        /// <param name="password">The password for authentication</param>
        /// <param name="port">The port number (default is 3306)</param>
        /// <param name="pooling">Whether to enable connection pooling (default is true)</param>
        /// <param name="minPoolSize">Minimum pool size (default is 0)</param>
        /// <param name="maxPoolSize">Maximum pool size (default is 100)</param>
        /// <param name="connectionTimeout">Connection timeout in seconds (default is 30)</param>
        /// <param name="commandTimeout">Command timeout in seconds (default is 30)</param>
        /// <param name="sslMode">SSL mode for secure connections (default is Preferred)</param>
        /// <returns>A configured MySQL connection factory</returns>
        public static MySqlConnectionFactory CreateMySqlFactory(
            string server,
            string database,
            string userId,
            string password,
            uint port = 3306,
            bool pooling = true,
            uint minPoolSize = 0,
            uint maxPoolSize = 100,
            uint connectionTimeout = 30,
            uint commandTimeout = 30,
            string sslMode = "Preferred")
        {
            if (string.IsNullOrWhiteSpace(server))
                throw new ArgumentException("Server cannot be null or empty", nameof(server));
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Database cannot be null or empty", nameof(database));
            if (string.IsNullOrWhiteSpace(userId))
                throw new ArgumentException("UserId cannot be null or empty", nameof(userId));

            MySqlConnector.MySqlConnectionStringBuilder builder = new MySqlConnector.MySqlConnectionStringBuilder
            {
                Server = server,
                Database = database,
                UserID = userId,
                Password = password,
                Port = port,
                Pooling = pooling,
                MinimumPoolSize = minPoolSize,
                MaximumPoolSize = maxPoolSize,
                ConnectionTimeout = connectionTimeout,
                DefaultCommandTimeout = commandTimeout,
                SslMode = Enum.Parse<MySqlConnector.MySqlSslMode>(sslMode, ignoreCase: true)
            };

            ConnectionPoolOptions? poolOptions = pooling ? new ConnectionPoolOptions
            {
                MinPoolSize = (int)minPoolSize,
                MaxPoolSize = (int)maxPoolSize,
                ConnectionTimeout = TimeSpan.FromSeconds(connectionTimeout)
            } : null;

            return new MySqlConnectionFactory(builder.ConnectionString, poolOptions);
        }

        /// <summary>
        /// Creates a MySQL connection factory for local development with default settings.
        /// </summary>
        /// <param name="database">The database name</param>
        /// <param name="userId">The user ID (default is "root")</param>
        /// <param name="password">The password (default is empty)</param>
        /// <param name="server">The server (default is "localhost")</param>
        /// <param name="port">The port (default is 3306)</param>
        /// <returns>A configured MySQL connection factory for local development</returns>
        public static MySqlConnectionFactory CreateLocalMySqlFactory(
            string database,
            string userId = "root",
            string password = "",
            string server = "localhost",
            uint port = 3306)
        {
            return CreateMySqlFactory(
                server: server,
                database: database,
                userId: userId,
                password: password,
                port: port,
                pooling: true,
                minPoolSize: 1,
                maxPoolSize: 10,
                sslMode: "None"
            );
        }

        /// <summary>
        /// Creates a MySQL connection factory configured for production use with recommended settings.
        /// </summary>
        /// <param name="server">The MySQL server hostname</param>
        /// <param name="database">The database name</param>
        /// <param name="userId">The user ID for authentication</param>
        /// <param name="password">The password for authentication</param>
        /// <param name="port">The port number (default is 3306)</param>
        /// <returns>A configured MySQL connection factory optimized for production</returns>
        public static MySqlConnectionFactory CreateProductionMySqlFactory(
            string server,
            string database,
            string userId,
            string password,
            uint port = 3306)
        {
            return CreateMySqlFactory(
                server: server,
                database: database,
                userId: userId,
                password: password,
                port: port,
                pooling: true,
                minPoolSize: 5,
                maxPoolSize: 50,
                connectionTimeout: 15,
                commandTimeout: 60,
                sslMode: "Required"
            );
        }
    }
}