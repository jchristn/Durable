namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Reads several result sets returned by one command, in order. Each <c>Read</c> call consumes the current result set
    /// and advances to the next. Dispose to release the reader and its connection.
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class SqlMultipleResultReader : IDisposable, IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets whether another result set is available.
        /// </summary>
        public bool HasMoreResults { get; private set; } = true;

        #endregion

        #region Private-Members

        private readonly ConnectionLease _Lease;
        private readonly DbCommand _Command;
        private readonly DbDataReader _Reader;
        private readonly IDataTypeConverter _Converter;
        private readonly Action? _OnDisposed;
        private bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        internal SqlMultipleResultReader(ConnectionLease lease, DbCommand command, DbDataReader reader, IDataTypeConverter converter, Action? onDisposed = null)
        {
            _OnDisposed = onDisposed;
            _Lease = lease;
            _Command = command;
            _Reader = reader;
            _Converter = converter;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Reads the current result set and advances to the next.
        /// </summary>
        /// <typeparam name="TResult">Row type: a class mapped by column name, or a scalar type read from the first column.</typeparam>
        /// <returns>The rows. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no result sets remain.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when disposed.</exception>
        public List<TResult> Read<TResult>()
        {
            ThrowIfUnavailable();
            List<TResult> rows = new List<TResult>();
            Func<DbDataReader, TResult> map = ResultMapper.Create<TResult>(_Converter);
            while (_Reader.Read()) rows.Add(map(_Reader));
            HasMoreResults = _Reader.NextResult();
            return rows;
        }

        /// <summary>
        /// Reads the current result set and advances to the next.
        /// </summary>
        /// <typeparam name="TResult">Row type.</typeparam>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The rows. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no result sets remain.</exception>
        public async Task<List<TResult>> ReadAsync<TResult>(CancellationToken token = default)
        {
            ThrowIfUnavailable();
            List<TResult> rows = new List<TResult>();
            Func<DbDataReader, TResult> map = ResultMapper.Create<TResult>(_Converter);
            while (await _Reader.ReadAsync(token).ConfigureAwait(false)) rows.Add(map(_Reader));
            HasMoreResults = await _Reader.NextResultAsync(token).ConfigureAwait(false);
            return rows;
        }

        /// <summary>
        /// Releases the reader, command and connection.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;
            _Reader.Dispose();
            _Command.Dispose();
            _Lease.Dispose();
            _OnDisposed?.Invoke();
        }

        /// <summary>
        /// Releases the reader, command and connection asynchronously.
        /// </summary>
        /// <returns>A task.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_Disposed) return;
            _Disposed = true;
            await _Reader.DisposeAsync().ConfigureAwait(false);
            await _Command.DisposeAsync().ConfigureAwait(false);
            await _Lease.DisposeAsync().ConfigureAwait(false);
            _OnDisposed?.Invoke();
        }

        #endregion

        #region Private-Methods

        private void ThrowIfUnavailable()
        {
            if (_Disposed) throw new ObjectDisposedException(nameof(SqlMultipleResultReader));
            if (!HasMoreResults) throw new InvalidOperationException("No more result sets are available.");
        }

        #endregion
    }
}
