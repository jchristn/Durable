namespace Durable.Sql
{
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A savepoint implemented with the ADO.NET driver's <see cref="DbTransaction"/> savepoint APIs.
    /// Used when a transaction was wrapped without a dialect.
    /// Thread safety: not thread-safe.
    /// </summary>
    internal sealed class DriverSavepoint : ISavepoint
    {
        private readonly DbTransaction _Transaction;

        internal DriverSavepoint(DbTransaction transaction, string name)
        {
            _Transaction = transaction;
            Name = name;
        }

        public string Name { get; }

        public void Release()
        {
            _Transaction.Release(Name);
        }

        public void Rollback()
        {
            _Transaction.Rollback(Name);
        }

        public Task ReleaseAsync(CancellationToken token = default)
        {
            return _Transaction.ReleaseAsync(Name, token);
        }

        public Task RollbackAsync(CancellationToken token = default)
        {
            return _Transaction.RollbackAsync(Name, token);
        }

        public void Dispose()
        {
        }
    }
}
