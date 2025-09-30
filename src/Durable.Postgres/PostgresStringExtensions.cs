namespace Durable.Postgres
{
    using System;

    /// <summary>
    /// Extension methods for string to provide PostgreSQL connection factory creation.
    /// Provides consistency with SQLite and MySQL implementations for connection string handling.
    /// </summary>
    public static class PostgresStringExtensions
    {
        /// <summary>
        /// Creates a PostgreSQL connection factory from a connection string.
        /// Provides a consistent API similar to SQLite's and MySQL's string extension methods.
        /// </summary>
        /// <param name="connectionString">The PostgreSQL connection string</param>
        /// <param name="configureOptions">Optional action to configure connection pool options</param>
        /// <returns>A configured PostgreSQL connection factory</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace</exception>
        public static PostgresConnectionFactory CreatePostgresFactory(this string connectionString, Action<ConnectionPoolOptions>? configureOptions = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionString));

            ConnectionPoolOptions options = new ConnectionPoolOptions();
            configureOptions?.Invoke(options);

            return new PostgresConnectionFactory(connectionString, options);
        }

        /// <summary>
        /// Creates a PostgreSQL connection factory from a connection string with default pooling options.
        /// </summary>
        /// <param name="connectionString">The PostgreSQL connection string</param>
        /// <returns>A PostgreSQL connection factory with default configuration</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace</exception>
        public static PostgresConnectionFactory CreateFactory(this string connectionString)
        {
            return CreatePostgresFactory(connectionString);
        }
    }
}