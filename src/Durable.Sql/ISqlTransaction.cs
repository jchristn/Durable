namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// A SQL transaction bound to one connection. Pass it to any repository of the same provider to run operations on
    /// that connection. Use <see cref="SqlTransactionContext.Wrap"/> to enlist a connection/transaction created elsewhere
    /// (for example by Dapper or EF Core) so Durable participates in it.
    /// Thread safety: not thread-safe; a connection supports one command at a time.
    /// </summary>
    public interface ISqlTransaction : ITransaction
    {
        /// <summary>
        /// Gets the connection. Never null.
        /// </summary>
        DbConnection Connection { get; }

        /// <summary>
        /// Gets the ADO.NET transaction, or null when wrapping a connection without a transaction.
        /// </summary>
        DbTransaction? Transaction { get; }

        /// <summary>
        /// Gets whether disposing this object disposes the connection and transaction.
        /// </summary>
        bool OwnsConnection { get; }

        /// <summary>
        /// Creates a savepoint.
        /// </summary>
        /// <param name="name">Savepoint name (letters, digits and underscores); null generates one.</param>
        /// <returns>The savepoint.</returns>
        /// <exception cref="InvalidOperationException">Thrown when there is no active transaction.</exception>
        /// <exception cref="ArgumentException">Thrown when name contains invalid characters.</exception>
        ISavepoint CreateSavepoint(string? name = null);

        /// <summary>
        /// Creates a savepoint.
        /// </summary>
        /// <param name="name">Savepoint name; null generates one.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The savepoint.</returns>
        /// <exception cref="InvalidOperationException">Thrown when there is no active transaction.</exception>
        Task<ISavepoint> CreateSavepointAsync(string? name = null, CancellationToken token = default);
    }
}
