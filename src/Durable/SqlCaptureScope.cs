namespace Durable
{
    using System;

    /// <summary>
    /// Disposable helper class that temporarily enables SQL capture and restores the original setting when disposed.
    /// </summary>
    internal sealed class SqlCaptureScope : IDisposable
    {
        #region Private-Members

        private readonly ISqlCapture _SqlCapture;
        private readonly bool _OriginalCaptureSetting;
        private bool _Disposed = false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlCaptureScope class.
        /// </summary>
        /// <param name="sqlCapture">The SQL capture instance to manage.</param>
        /// <exception cref="ArgumentNullException">Thrown when sqlCapture is null.</exception>
        public SqlCaptureScope(ISqlCapture sqlCapture)
        {
            ArgumentNullException.ThrowIfNull(sqlCapture);

            _SqlCapture = sqlCapture;
            _OriginalCaptureSetting = sqlCapture.CaptureSql;
            sqlCapture.CaptureSql = true;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Restores the original SQL capture setting.
        /// </summary>
        public void Dispose()
        {
            if (!_Disposed)
            {
                _SqlCapture.CaptureSql = _OriginalCaptureSetting;
                _Disposed = true;
            }
        }

        #endregion
    }
}