namespace Durable
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IConnectionFactory : IDisposable
    {
        DbConnection GetConnection();
        Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
        void ReturnConnection(DbConnection connection);
        Task ReturnConnectionAsync(DbConnection connection);
    }

    public class ConnectionPoolOptions
    {
        public int MinPoolSize { get; set; } = 5;
        public int MaxPoolSize { get; set; } = 100;
        public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(10);
        public bool ValidateConnections { get; set; } = true;
    }
}