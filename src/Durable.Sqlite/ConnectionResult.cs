namespace Durable.Sqlite
{
    using Microsoft.Data.Sqlite;

    internal class ConnectionResult
    {
        public SqliteConnection Connection { get; }
        public SqliteCommand Command { get; }
        public bool ShouldDispose { get; }

        public ConnectionResult(SqliteConnection connection, SqliteCommand command, bool shouldDispose)
        {
            Connection = connection;
            Command = command;
            ShouldDispose = shouldDispose;
        }
    }
}