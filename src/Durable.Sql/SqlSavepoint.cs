namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A savepoint implemented with dialect SQL on the transaction's connection.
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class SqlSavepoint : ISavepoint
    {
        #region Public-Members

        /// <inheritdoc />
        public string Name { get; }

        #endregion

        #region Private-Members

        private readonly ISqlTransaction _Transaction;
        private readonly ISqlDialect _Dialect;

        #endregion

        #region Constructors-and-Factories

        internal SqlSavepoint(ISqlTransaction transaction, ISqlDialect dialect, string name)
        {
            _Transaction = transaction;
            _Dialect = dialect;
            Name = name;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public void Release()
        {
            string? sql = _Dialect.ReleaseSavepointSql(Name);
            if (sql != null) Execute(sql);
        }

        /// <inheritdoc />
        public void Rollback()
        {
            Execute(_Dialect.RollbackToSavepointSql(Name));
        }

        /// <inheritdoc />
        public Task ReleaseAsync(CancellationToken token = default)
        {
            string? sql = _Dialect.ReleaseSavepointSql(Name);
            return sql == null ? Task.CompletedTask : ExecuteAsync(sql, token);
        }

        /// <inheritdoc />
        public Task RollbackAsync(CancellationToken token = default)
        {
            return ExecuteAsync(_Dialect.RollbackToSavepointSql(Name), token);
        }

        /// <summary>
        /// Does nothing; savepoints end with their transaction.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        internal void Create()
        {
            Execute(_Dialect.CreateSavepointSql(Name));
        }

        internal Task CreateAsync(CancellationToken token)
        {
            return ExecuteAsync(_Dialect.CreateSavepointSql(Name), token);
        }

        private void Execute(string sql)
        {
            using DbCommand command = _Transaction.Connection.CreateCommand();
            command.Transaction = _Transaction.Transaction;
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private async Task ExecuteAsync(string sql, CancellationToken token)
        {
            DbCommand command = _Transaction.Connection.CreateCommand();
            await using (command.ConfigureAwait(false))
            {
                command.Transaction = _Transaction.Transaction;
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
