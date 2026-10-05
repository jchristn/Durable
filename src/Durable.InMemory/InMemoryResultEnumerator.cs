namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Enumerator of an <see cref="InMemoryResultStream"/>; every step completes synchronously.
    /// Thread safety: not thread-safe.
    /// </summary>
    internal sealed class InMemoryResultEnumerator : IAsyncEnumerator<object>
    {
        #region Public-Members

        /// <inheritdoc />
        public object Current => _Current ?? throw new InvalidOperationException("Enumeration has not started or has finished.");

        #endregion

        #region Private-Members

        private readonly IReadOnlyList<InMemoryRow> _Rows;
        private readonly Func<InMemoryRow, object> _Materialize;
        private readonly CancellationToken _QueryToken;
        private readonly CancellationToken _EnumerationToken;
        private int _Index = -1;
        private object? _Current;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an enumerator.
        /// </summary>
        /// <param name="rows">Rows. Must not be null.</param>
        /// <param name="materialize">Materializer. Must not be null.</param>
        /// <param name="queryToken">Token supplied with the query.</param>
        /// <param name="enumerationToken">Token supplied to the enumeration.</param>
        public InMemoryResultEnumerator(IReadOnlyList<InMemoryRow> rows, Func<InMemoryRow, object> materialize, CancellationToken queryToken, CancellationToken enumerationToken)
        {
            _Rows = rows;
            _Materialize = materialize;
            _QueryToken = queryToken;
            _EnumerationToken = enumerationToken;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public ValueTask<bool> MoveNextAsync()
        {
            _QueryToken.ThrowIfCancellationRequested();
            _EnumerationToken.ThrowIfCancellationRequested();
            _Index++;
            if (_Index >= _Rows.Count)
            {
                _Current = null;
                return new ValueTask<bool>(false);
            }

            _Current = _Materialize(_Rows[_Index]);
            return new ValueTask<bool>(true);
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            _Current = null;
            return ValueTask.CompletedTask;
        }

        #endregion
    }
}
