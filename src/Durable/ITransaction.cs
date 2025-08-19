namespace Durable
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ITransaction : IDisposable
    {
        DbConnection Connection { get; }
        DbTransaction Transaction { get; }
        void Commit();
        void Rollback();
        Task CommitAsync(CancellationToken token = default);
        Task RollbackAsync(CancellationToken token = default);
    }
}