namespace Durable.Oracle
{
    using System;
    using System.Data.Common;
    using Durable.Sql;
    using global::Oracle.ManagedDataAccess.Client;

    /// <summary>
    /// Opens Oracle connections, relying on ODP.NET's built-in pooling. Every connection binds parameters by name
    /// (<see cref="OracleConnection.BindByName"/>) and truncates NUMBER values that exceed <see cref="decimal"/> precision
    /// instead of throwing (<see cref="OracleConnection.SuppressGetDecimalInvalidCastException"/>), which Durable relies on.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public sealed class OracleConnectionFactory : ConnectionFactory
    {
        /// <summary>
        /// Gets the connection string. Never null.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Instantiates the factory from strongly-typed settings (<see cref="OracleRepositorySettings.BuildConnectionString"/>).
        /// </summary>
        /// <param name="settings">Settings. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when settings is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the settings lack required values (see <see cref="OracleRepositorySettings.BuildConnectionString"/>).</exception>
        public OracleConnectionFactory(OracleRepositorySettings settings, int? maxConcurrentConnections = null)
            : this(BuildConnectionString(settings), maxConcurrentConnections)
        {
        }

        /// <summary>
        /// Instantiates the factory.
        /// </summary>
        /// <param name="connectionString">ODP.NET connection string. Must not be null.</param>
        /// <param name="maxConcurrentConnections">Optional cap on concurrently open connections; null for none.</param>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public OracleConnectionFactory(string connectionString, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <inheritdoc />
        protected override DbConnection CreateConnection()
        {
            return CreateRawConnection(ConnectionString);
        }

        /// <summary>
        /// Creates an unopened <see cref="OracleConnection"/> configured the way Durable expects (bind by name, NUMBER
        /// values truncated to <see cref="decimal"/> precision). Use it for connections handed to
        /// <see cref="SqlTransactionContext.Wrap"/>.
        /// </summary>
        /// <param name="connectionString">ODP.NET connection string. Must not be null.</param>
        /// <returns>The connection; the caller owns it.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        public static OracleConnection CreateRawConnection(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            return new OracleConnection(connectionString)
            {
                BindByName = true,
                SuppressGetDecimalInvalidCastException = true
            };
        }

        private static string BuildConnectionString(OracleRepositorySettings settings)
        {
            ArgumentNullException.ThrowIfNull(settings);
            return settings.BuildConnectionString();
        }
    }
}
