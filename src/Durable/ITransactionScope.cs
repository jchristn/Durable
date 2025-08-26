namespace Durable
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ITransactionScope : IDisposable
    {
        ITransaction Transaction { get; }
        bool IsCompleted { get; }
        void Complete();
        Task CompleteAsync(CancellationToken token = default);
    }
}