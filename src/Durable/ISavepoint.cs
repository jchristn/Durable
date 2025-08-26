namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ISavepoint : IDisposable
    {
        string Name { get; }
        void Release();
        void Rollback();
        Task ReleaseAsync(CancellationToken token = default);
        Task RollbackAsync(CancellationToken token = default);
    }
}