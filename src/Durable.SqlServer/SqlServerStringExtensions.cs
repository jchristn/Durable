namespace Durable.SqlServer
{
    using System;

    /// <summary>
    /// Extension methods for string to provide MySQL connection factory creation.
    /// Provides consistency with SQLite implementation for connection string handling.
    /// </summary>
    public static class SqlServerStringExtensions
    {
        /// <summary>
        /// Creates a MySQL connection factory from a connection string.
        /// Provides a consistent API similar to SQLite's string extension method.
        /// </summary>
        /// <param name="connectionString">The MySQL connection string</param>
        /// <param name="configureOptions">Optional action to configure connection pool options</param>
        /// <returns>A configured MySQL connection factory</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace</exception>
        public static SqlServerConnectionFactory CreateSqlServerFactory(this string connectionString, Action<ConnectionPoolOptions>? configureOptions = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionString));

            ConnectionPoolOptions options = new ConnectionPoolOptions();
            configureOptions?.Invoke(options);

            return new SqlServerConnectionFactory(connectionString, options);
        }

        /// <summary>
        /// Creates a MySQL connection factory from a connection string with default pooling options.
        /// </summary>
        /// <param name="connectionString">The MySQL connection string</param>
        /// <returns>A MySQL connection factory with default configuration</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace</exception>
        public static SqlServerConnectionFactory CreateFactory(this string connectionString)
        {
            return CreateSqlServerFactory(connectionString);
        }
    }
}