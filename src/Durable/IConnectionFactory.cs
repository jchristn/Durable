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
}