namespace Durable
{
    using System.Data;

    /// <summary>
    /// Represents the result of retrieving a database connection and command.
    /// </summary>
    /// <typeparam name="TConnection">The type of database connection.</typeparam>
    /// <typeparam name="TCommand">The type of database command.</typeparam>
    public sealed class ConnectionCommandResult<TConnection, TCommand>
        where TConnection : IDbConnection
        where TCommand : IDbCommand
    {
        /// <summary>
        /// Gets or sets the database connection.
        /// </summary>
        public TConnection Connection { get; set; }

        /// <summary>
        /// Gets or sets the database command.
        /// </summary>
        public TCommand Command { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the connection should be returned to the pool when disposed.
        /// </summary>
        public bool ShouldReturnToPool { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionCommandResult{TConnection, TCommand}"/> class.
        /// </summary>
        public ConnectionCommandResult()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionCommandResult{TConnection, TCommand}"/> class.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        /// <param name="command">The database command.</param>
        /// <param name="shouldReturnToPool">Whether the connection should be returned to the pool.</param>
        public ConnectionCommandResult(TConnection connection, TCommand command, bool shouldReturnToPool)
        {
            Connection = connection;
            Command = command;
            ShouldReturnToPool = shouldReturnToPool;
        }
    }
}
