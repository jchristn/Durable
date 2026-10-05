namespace Durable.Sql
{
    using System;
    using System.Threading;

    /// <summary>
    /// Captures the last SQL statement executed on the current async flow, regardless of repository CaptureSql settings.
    /// Begin a scope from synchronous code (or before awaiting) so nested async calls record into it.
    /// Thread safety: scopes flow with the async execution context.
    /// </summary>
    public sealed class SqlCaptureScope : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the innermost active scope on this flow, or null.
        /// </summary>
        public static SqlCaptureScope? Current => _Current.Value;

        /// <summary>
        /// Gets the last statement executed while this scope was active, or null.
        /// </summary>
        public SqlStatement? LastStatement { get; internal set; }

        #endregion

        #region Private-Members

        private static readonly AsyncLocal<SqlCaptureScope?> _Current = new AsyncLocal<SqlCaptureScope?>();
        private readonly SqlCaptureScope? _Parent;
        private bool _Disposed;

        #endregion

        #region Constructors-and-Factories

        private SqlCaptureScope(SqlCaptureScope? parent)
        {
            _Parent = parent;
        }

        /// <summary>
        /// Starts a capture scope on the current flow.
        /// </summary>
        /// <returns>The scope; dispose it to stop capturing.</returns>
        public static SqlCaptureScope Begin()
        {
            SqlCaptureScope scope = new SqlCaptureScope(_Current.Value);
            _Current.Value = scope;
            return scope;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Ends the scope and restores the parent scope.
        /// </summary>
        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;
            if (ReferenceEquals(_Current.Value, this)) _Current.Value = _Parent;
        }

        #endregion

        #region Private-Methods

        internal void Record(SqlStatement statement)
        {
            SqlCaptureScope? scope = this;
            while (scope != null)
            {
                if (!scope._Disposed) scope.LastStatement = statement;
                scope = scope._Parent;
            }
        }

        #endregion
    }
}
