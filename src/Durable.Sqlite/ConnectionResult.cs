namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Internal helper class that manages the lifecycle of SQLite connections and commands.
    /// Ensures proper disposal of database resources.
    /// </summary>
    internal class ConnectionResult
    {
        /// <summary>
        /// Gets the SQLite connection associated with this result.
        /// </summary>
        public SqliteConnection Connection { get; }

        /// <summary>
        /// Gets the SQLite command associated with this result.
        /// </summary>
        public SqliteCommand Command { get; }

        /// <summary>
        /// Gets a value indicating whether this connection result should dispose the connection when disposed.
        /// </summary>
        public bool ShouldDispose { get; }

        /// <summary>
        /// Initializes a new instance of the ConnectionResult.
        /// </summary>
        /// <param name="connection">The SQLite connection</param>
        /// <param name="command">The SQLite command</param>
        /// <param name="shouldDispose">Whether to dispose the connection when this object is disposed</param>
        public ConnectionResult(SqliteConnection connection, SqliteCommand command, bool shouldDispose)
        {
            Connection = connection;
            Command = command;
            ShouldDispose = shouldDispose;
        }
    }
}