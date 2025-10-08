namespace Durable
{
    using System;
    using System.Data.Common;

    /// <summary>
    /// Represents a database connection wrapper used internally by the ConnectionPool to track connection state and usage.
    /// </summary>
    internal class PooledConnection
    {

        #region Public-Members

        /// <summary>
        /// Gets the underlying database connection.
        /// </summary>
        public DbConnection Connection { get; }

        /// <summary>
        /// Gets the UTC timestamp when this connection was created.
        /// </summary>
        public DateTime Created { get; }

        /// <summary>
        /// Gets or sets the UTC timestamp when this connection was last used.
        /// </summary>
        public DateTime LastUsed { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this connection is currently in use.
        /// </summary>
        public bool IsInUse { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PooledConnection class.
        /// </summary>
        /// <param name="connection">The database connection to wrap.</param>
        /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
        public PooledConnection(DbConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            Created = DateTime.UtcNow;
            LastUsed = DateTime.UtcNow;
            IsInUse = false;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion

    }
}
