namespace Durable.Sqlite
{
    using System;

    public static class SqliteConnectionFactoryExtensions
    {
        public static SqliteConnectionFactory CreateFactory(this string connectionString, Action<ConnectionPoolOptions> configureOptions = null)
        {
            ConnectionPoolOptions options = new ConnectionPoolOptions();
            configureOptions?.Invoke(options);
            return new SqliteConnectionFactory(connectionString, options);
        }
    }
}