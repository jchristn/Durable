namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// An asynchronous stream over rows selected from a snapshot. Every step completes synchronously, so synchronous
    /// callers never block or hop threads. Entities are materialized lazily as the stream is read.
    /// Thread safety: each enumeration is independent; an enumerator is not thread-safe.
    /// </summary>
    internal sealed class InMemoryResultStream : IAsyncEnumerable<object>
    {
        #region Private-Members

        private readonly IReadOnlyList<InMemoryRow> _Rows;
        private readonly Func<InMemoryRow, object> _Materialize;
        private readonly CancellationToken _Token;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a stream.
        /// </summary>
        /// <param name="rows">Rows in result order. Must not be null.</param>
        /// <param name="materialize">Creates an entity from a row. Must not be null.</param>
        /// <param name="token">Cancellation token supplied when the query was issued.</param>
        public InMemoryResultStream(IReadOnlyList<InMemoryRow> rows, Func<InMemoryRow, object> materialize, CancellationToken token)
        {
            _Rows = rows ?? throw new ArgumentNullException(nameof(rows));
            _Materialize = materialize ?? throw new ArgumentNullException(nameof(materialize));
            _Token = token;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public IAsyncEnumerator<object> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new InMemoryResultEnumerator(_Rows, _Materialize, _Token, cancellationToken);
        }

        #endregion
    }
}
