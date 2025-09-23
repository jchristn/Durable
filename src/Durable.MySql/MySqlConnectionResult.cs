namespace Durable.MySql
{
    using System;
    using System.Data.Common;

    /// <summary>
    /// Internal helper class that manages the lifecycle of MySQL connections and commands.
    /// Ensures proper disposal of database resources.
    /// </summary>
    internal class MySqlConnectionResult : IDisposable
    {
        #region Private-Members

        private bool _Disposed = false;

        #endregion

        #region Public-Members

        /// <summary>
        /// Gets the MySQL connection associated with this result.
        /// </summary>
        public MySqlConnector.MySqlConnection Connection { get; private set; }

        /// <summary>
        /// Gets the MySQL command associated with this result.
        /// </summary>
        public MySqlConnector.MySqlCommand Command { get; private set; }

        /// <summary>
        /// Gets a value indicating whether this connection result should dispose the connection when disposed.
        /// </summary>
        public bool ShouldDisposeConnection { get; private set; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlConnectionResult.
        /// </summary>
        /// <param name="connection">The MySQL connection</param>
        /// <param name="command">The MySQL command</param>
        /// <param name="shouldDisposeConnection">Whether to dispose the connection when this object is disposed</param>
        /// <exception cref="ArgumentNullException">Thrown when connection or command is null</exception>
        public MySqlConnectionResult(MySqlConnector.MySqlConnection connection, MySqlConnector.MySqlCommand command, bool shouldDisposeConnection = true)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            Command = command ?? throw new ArgumentNullException(nameof(command));
            ShouldDisposeConnection = shouldDisposeConnection;
        }

        /// <summary>
        /// Creates a new MySqlConnectionResult with a connection and command that should be disposed together.
        /// </summary>
        /// <param name="connection">The MySQL connection</param>
        /// <param name="command">The MySQL command</param>
        /// <returns>A new MySqlConnectionResult instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection or command is null</exception>
        public static MySqlConnectionResult Create(MySqlConnector.MySqlConnection connection, MySqlConnector.MySqlCommand command)
        {
            return new MySqlConnectionResult(connection, command, true);
        }

        /// <summary>
        /// Creates a new MySqlConnectionResult where the connection should not be disposed (e.g., when using transactions).
        /// </summary>
        /// <param name="connection">The MySQL connection</param>
        /// <param name="command">The MySQL command</param>
        /// <returns>A new MySqlConnectionResult instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection or command is null</exception>
        public static MySqlConnectionResult CreateWithManagedConnection(MySqlConnector.MySqlConnection connection, MySqlConnector.MySqlCommand command)
        {
            return new MySqlConnectionResult(connection, command, false);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes the command and optionally the connection based on the ShouldDisposeConnection setting.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Performs the actual disposal of resources.
        /// </summary>
        /// <param name="disposing">True if disposing managed resources, false if called from finalizer</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_Disposed && disposing)
            {
                try
                {
                    // Always dispose the command
                    Command?.Dispose();
                }
                catch (Exception)
                {
                    // Ignore disposal exceptions
                }

                try
                {
                    // Only dispose connection if we're supposed to
                    if (ShouldDisposeConnection)
                    {
                        Connection?.Dispose();
                    }
                }
                catch (Exception)
                {
                    // Ignore disposal exceptions
                }

                _Disposed = true;
            }
        }

        /// <summary>
        /// Finalizer to ensure resources are cleaned up if Dispose is not called.
        /// </summary>
        ~MySqlConnectionResult()
        {
            Dispose(false);
        }

        #endregion
    }
}