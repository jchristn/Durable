namespace Durable.MySql
{
    using System;
    using System.Data.Common;
    using MySqlConnector;
    using Durable.Sql;

    /// <summary>
    /// Opens MySQL connections through MySqlConnector's pooling, either from a connection string or a caller-supplied
    /// <see cref="MySqlDataSource"/>.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public sealed class MySqlConnectionFactory : ConnectionFactory
    {
        #region Public-Members

        /// <summary>
        /// Gets the data source supplied by the caller, or null when the factory uses a connection string
        /// (connections then come from MySqlConnector's shared per-connection-string pool).
        /// </summary>
        public MySqlDataSource? DataSource { get; }

        /// <summary>
        /// Gets the connection string, or null when the factory wraps a caller-supplied data source.
        /// </summary>
        public string? ConnectionString { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the factory from a connection string. Connections share MySqlConnector's pool for that connection
        /// string, so any number of factories and repositories with the same connection string use one pool.
        /// </summary>
        /// <param name="connectionString">MySqlConnector connection string. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public MySqlConnectionFactory(string connectionString, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <summary>
        /// Instantiates the factory over an existing data source, which is not disposed with the factory.
        /// </summary>
        /// <param name="dataSource">Data source. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when dataSource is null.</exception>
        public MySqlConnectionFactory(MySqlDataSource dataSource, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            DataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override DbConnection CreateConnection()
        {
            return DataSource != null ? DataSource.CreateConnection() : new MySqlConnection(ConnectionString);
        }

        #endregion
    }
}
