namespace Durable.SqlServer
{
    using System;
    using System.Data.Common;
    using Microsoft.Data.SqlClient;
    using Durable.Sql;

    /// <summary>
    /// Opens SQL Server connections, relying on SqlClient's built-in pooling.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public sealed class SqlServerConnectionFactory : ConnectionFactory
    {
        /// <summary>
        /// Gets the connection string. Never null.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Instantiates the factory from strongly-typed settings (<see cref="SqlServerRepositorySettings.BuildConnectionString"/>).
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the settings lack required values (see <see cref="SqlServerRepositorySettings.BuildConnectionString"/>).</exception>
        public SqlServerConnectionFactory(SqlServerRepositorySettings settings, int? maxConcurrentConnections = null)
            : this(BuildConnectionString(settings), maxConcurrentConnections)
        {
        }

        /// <summary>
        /// Instantiates the factory.
        /// </summary>
        /// <param name="connectionString">SqlClient connection string. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public SqlServerConnectionFactory(string connectionString, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <inheritdoc />
        protected override DbConnection CreateConnection()
        {
            return new SqlConnection(ConnectionString);
        }

        private static string BuildConnectionString(SqlServerRepositorySettings settings)
        {
            ArgumentNullException.ThrowIfNull(settings);
            return settings.BuildConnectionString();
        }
    }
}
