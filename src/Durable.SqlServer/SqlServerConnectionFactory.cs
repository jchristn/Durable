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
    }
}
