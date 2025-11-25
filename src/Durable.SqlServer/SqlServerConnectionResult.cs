namespace Durable.SqlServer
{
    using System;
    using System.Data.Common;
    using Microsoft.Data.SqlClient;

    /// <summary>
    /// Internal helper class that represents a SQL Server database connection with its associated factory for proper resource management.
    /// Ensures connections are returned to the pool when disposed.
    /// </summary>
    internal class SqlServerConnectionResult : IDisposable
    {

        #region Public-Members

        /// <summary>
        /// Gets the SQL Server database connection.
        /// </summary>
        public SqlConnection Connection { get; }

        #endregion

        #region Private-Members

        private readonly IConnectionFactory _Factory;
        private volatile bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlServerConnectionResult class.
        /// </summary>
        /// <param name="connection">The SQL Server database connection to wrap.</param>
        /// <param name="factory">The connection factory that created this connection, used for returning it to the pool.</param>
        /// <exception cref="ArgumentNullException">Thrown when connection or factory is null.</exception>
        public SqlServerConnectionResult(SqlConnection connection, IConnectionFactory factory)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _Factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes the connection result and returns the connection to the factory pool.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed)
                return;

            _Disposed = true;

            try
            {
                _Factory.ReturnConnection(Connection);
            }
            catch
            {
                // If returning to pool fails, dispose the connection directly
                Connection?.Dispose();
            }
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
