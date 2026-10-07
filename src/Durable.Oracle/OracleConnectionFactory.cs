namespace Durable.Oracle
{
    using System;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using Durable.Sql;
    using global::Oracle.ManagedDataAccess.Client;

    /// <summary>
    /// Opens Oracle connections, relying on ODP.NET's built-in pooling. Every connection binds parameters by name
    /// (<see cref="OracleConnection.BindByName"/>) and truncates NUMBER values that exceed <see cref="decimal"/> precision
    /// instead of throwing (<see cref="OracleConnection.SuppressGetDecimalInvalidCastException"/>), which Durable relies on.
    /// Trimming and Native AOT: the ODP.NET managed driver is not annotated for trimming and produces trim and AOT analysis
    /// warnings when published (type names resolved by string, UDT assembly scanning, Assembly.Location in configuration
    /// tracing), so the constructors and <see cref="CreateRawConnection"/> carry <see cref="RequiresUnreferencedCodeAttribute"/>
    /// and <see cref="RequiresDynamicCodeAttribute"/>. Durable's own code in this package is trim-annotated and warning-free.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    public sealed class OracleConnectionFactory : ConnectionFactory
    {
        internal const string DriverTrimWarning = "The ODP.NET managed driver (Oracle.ManagedDataAccess.Core) is not annotated for trimming and produces trim and Native AOT analysis warnings; test trimmed and native AOT applications against your database.";

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
        [RequiresUnreferencedCode(DriverTrimWarning)]
        [RequiresDynamicCode(DriverTrimWarning)]
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
        [RequiresUnreferencedCode(DriverTrimWarning)]
        [RequiresDynamicCode(DriverTrimWarning)]
        public OracleConnectionFactory(string connectionString, int? maxConcurrentConnections = null) : base(maxConcurrentConnections)
        {
            ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <inheritdoc />
        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Every OracleConnectionFactory is created through a constructor that carries RequiresUnreferencedCode for the driver, so callers were already warned.")]
        [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Every OracleConnectionFactory is created through a constructor that carries RequiresDynamicCode for the driver, so callers were already warned.")]
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
        [RequiresUnreferencedCode(DriverTrimWarning)]
        [RequiresDynamicCode(DriverTrimWarning)]
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
