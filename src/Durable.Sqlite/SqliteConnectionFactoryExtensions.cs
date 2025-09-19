namespace Durable.Sqlite
{
    using System;

    /// <summary>
    /// Extension methods for creating SQLite connection factories.
    /// </summary>
    public static class SqliteConnectionFactoryExtensions
    {
        /// <summary>
        /// Creates a SQLite connection factory from a connection string with optional configuration.
        /// </summary>
        /// <param name="connectionString">The SQLite connection string.</param>
        /// <param name="configureOptions">Optional action to configure connection pool options.</param>
        /// <returns>A configured SQLite connection factory.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public static SqliteConnectionFactory CreateFactory(this string connectionString, Action<ConnectionPoolOptions> configureOptions = null)
        {
            if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));

            ConnectionPoolOptions options = new ConnectionPoolOptions();
            configureOptions?.Invoke(options);
            return new SqliteConnectionFactory(connectionString, options);
        }
    }
}