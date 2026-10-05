namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Durable;

    /// <summary>
    /// Executes statements for a repository: resolves the connection (explicit transaction, ambient
    /// <see cref="TransactionScope"/>, or a new connection), binds parameters, applies the command timeout, and runs
    /// interceptors, logging, tracing and SQL capture around every command.
    /// Thread safety: safe for concurrent use; each call uses its own command.
    /// </summary>
    public sealed class SqlCommandExecutor
    {
        #region Public-Members

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets the connection factory. Never null.
        /// </summary>
        public IConnectionFactory ConnectionFactory { get; }

        /// <summary>
        /// Gets the options. Never null.
        /// </summary>
        public SqlRepositoryOptions Options { get; }

        /// <summary>
        /// Gets the ADO.NET connection type this executor accepts in transactions. Never null.
        /// </summary>
        public Type ConnectionType { get; }

        #endregion

        #region Private-Members

        private readonly Type _EntityType;
        private readonly string _TableName;
        private readonly Action<SqlStatement> _OnExecuted;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an executor.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="connectionFactory">Connection factory. Must not be null.</param>
        /// <param name="options">Options. Must not be null.</param>
        /// <param name="connectionType">ADO.NET connection type of the provider. Must not be null.</param>
        /// <param name="entityType">Entity type for diagnostics. Must not be null.</param>
        /// <param name="tableName">Table name for diagnostics. Must not be null.</param>
        /// <param name="onExecuted">Callback receiving each executed statement (for SQL capture). Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SqlCommandExecutor(ISqlDialect dialect, IConnectionFactory connectionFactory, SqlRepositoryOptions options, Type connectionType, Type entityType, string tableName, Action<SqlStatement> onExecuted)
        {
            Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            ConnectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            Options = options ?? throw new ArgumentNullException(nameof(options));
            ConnectionType = connectionType ?? throw new ArgumentNullException(nameof(connectionType));
            _EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            _TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _OnExecuted = onExecuted ?? throw new ArgumentNullException(nameof(onExecuted));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Resolves the transaction to use: the explicit one, else a compatible ambient <see cref="TransactionScope"/>, else null.
        /// </summary>
        /// <param name="transaction">Explicit transaction; may be null.</param>
        /// <returns>The SQL transaction, or null.</returns>
        /// <exception cref="ArgumentException">Thrown when the explicit transaction belongs to another provider.</exception>
        public ISqlTransaction? ResolveTransaction(ITransaction? transaction)
        {
            if (transaction != null)
            {
                if (transaction is not ISqlTransaction sqlTransaction)
                    throw new ArgumentException("The transaction was not created by a SQL repository.", nameof(transaction));
                if (!ConnectionType.IsInstanceOfType(sqlTransaction.Connection))
                    throw new ArgumentException(
                        "The transaction's connection (" + sqlTransaction.Connection.GetType().Name + ") is not a " + ConnectionType.Name +
                        "; a transaction cannot span database providers.", nameof(transaction));
                return sqlTransaction;
            }

            TransactionScope? scope = TransactionScope.Current;
            if (scope != null && scope.Transaction is ISqlTransaction ambient && !ambient.IsCompleted && ConnectionType.IsInstanceOfType(ambient.Connection))
                return ambient;
            return null;
        }

        /// <summary>
        /// Leases a connection for an operation.
        /// </summary>
        /// <param name="transaction">Explicit transaction; may be null.</param>
        /// <returns>The lease.</returns>
        public ConnectionLease Lease(ITransaction? transaction)
        {
            ISqlTransaction? resolved = ResolveTransaction(transaction);
            if (resolved != null) return new ConnectionLease(resolved.Connection, resolved);
            return new ConnectionLease(ConnectionFactory.OpenConnection(), null);
        }

        /// <summary>
        /// Leases a connection for an operation.
        /// </summary>
        /// <param name="transaction">Explicit transaction; may be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The lease.</returns>
        public async Task<ConnectionLease> LeaseAsync(ITransaction? transaction, CancellationToken token)
        {
            ISqlTransaction? resolved = ResolveTransaction(transaction);
            if (resolved != null) return new ConnectionLease(resolved.Connection, resolved);
            DbConnection connection = await ConnectionFactory.OpenConnectionAsync(token).ConfigureAwait(false);
            return new ConnectionLease(connection, null);
        }

        /// <summary>
        /// Creates a command on a lease with parameters bound and timeout applied.
        /// </summary>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <returns>The command; the caller disposes it.</returns>
        public DbCommand CreateCommand(ConnectionLease lease, SqlStatement statement)
        {
            ArgumentNullException.ThrowIfNull(lease);
            ArgumentNullException.ThrowIfNull(statement);
            DbCommand command = lease.Connection.CreateCommand();
            command.Transaction = lease.Transaction;
            command.CommandText = statement.Sql;
            if (Options.CommandTimeoutSeconds.HasValue) command.CommandTimeout = Options.CommandTimeoutSeconds.Value;
            IReadOnlyList<SqlParameterValue> parameters = statement.Parameters;
            for (int i = 0; i < parameters.Count; i++)
            {
                SqlParameterValue value = parameters[i];
                DbParameter parameter = command.CreateParameter();
                parameter.ParameterName = value.Name;
                parameter.Value = value.Value ?? DBNull.Value;
                parameter.Direction = value.Direction;
                if (value.DbType.HasValue) parameter.DbType = value.DbType.Value;
                Dialect.ConfigureParameter(parameter, value);
                command.Parameters.Add(parameter);
            }

            return command;
        }

        /// <summary>
        /// Executes a statement that returns no rows.
        /// </summary>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>Rows affected.</returns>
        public int ExecuteNonQuery(SqlStatement statement, ITransaction? transaction, string operation, CommandType commandType = CommandType.Text)
        {
            using ConnectionLease lease = Lease(transaction);
            return ExecuteNonQuery(lease, statement, operation, commandType);
        }

        /// <summary>
        /// Executes a statement that returns no rows on an existing lease.
        /// </summary>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>Rows affected.</returns>
        public int ExecuteNonQuery(ConnectionLease lease, SqlStatement statement, string operation, CommandType commandType = CommandType.Text)
        {
            using DbCommand command = CreateCommand(lease, statement);
            command.CommandType = commandType;
            CommandScope scope = Begin(command, statement, operation);
            try
            {
                int rows = command.ExecuteNonQuery();
                CopyOutputParameters(command, statement);
                scope.Complete(rows);
                return rows;
            }
            catch (Exception e)
            {
                scope.Fail(e);
                throw;
            }
        }

        /// <summary>
        /// Executes a statement that returns no rows.
        /// </summary>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>Rows affected.</returns>
        public async Task<int> ExecuteNonQueryAsync(SqlStatement statement, ITransaction? transaction, string operation, CancellationToken token, CommandType commandType = CommandType.Text)
        {
            ConnectionLease lease = await LeaseAsync(transaction, token).ConfigureAwait(false);
            await using (lease.ConfigureAwait(false))
            {
                return await ExecuteNonQueryAsync(lease, statement, operation, token, commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Executes a statement that returns no rows on an existing lease.
        /// </summary>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>Rows affected.</returns>
        public async Task<int> ExecuteNonQueryAsync(ConnectionLease lease, SqlStatement statement, string operation, CancellationToken token, CommandType commandType = CommandType.Text)
        {
            DbCommand command = CreateCommand(lease, statement);
            await using (command.ConfigureAwait(false))
            {
                command.CommandType = commandType;
                CommandScope scope = Begin(command, statement, operation);
                try
                {
                    int rows = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    CopyOutputParameters(command, statement);
                    scope.Complete(rows);
                    return rows;
                }
                catch (Exception e)
                {
                    scope.Fail(e);
                    throw;
                }
            }
        }

        /// <summary>
        /// Executes a statement and returns the first column of the first row.
        /// </summary>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <returns>The value, or null when no row or a database null.</returns>
        public object? ExecuteScalar(SqlStatement statement, ITransaction? transaction, string operation)
        {
            using ConnectionLease lease = Lease(transaction);
            using DbCommand command = CreateCommand(lease, statement);
            CommandScope scope = Begin(command, statement, operation);
            try
            {
                object? value = command.ExecuteScalar();
                scope.Complete(null);
                return value == DBNull.Value ? null : value;
            }
            catch (Exception e)
            {
                scope.Fail(e);
                throw;
            }
        }

        /// <summary>
        /// Executes a statement and returns the first column of the first row.
        /// </summary>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The value, or null when no row or a database null.</returns>
        public async Task<object?> ExecuteScalarAsync(SqlStatement statement, ITransaction? transaction, string operation, CancellationToken token)
        {
            ConnectionLease lease = await LeaseAsync(transaction, token).ConfigureAwait(false);
            await using (lease.ConfigureAwait(false))
            {
                DbCommand command = CreateCommand(lease, statement);
                await using (command.ConfigureAwait(false))
                {
                    CommandScope scope = Begin(command, statement, operation);
                    try
                    {
                        object? value = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
                        scope.Complete(null);
                        return value == DBNull.Value ? null : value;
                    }
                    catch (Exception e)
                    {
                        scope.Fail(e);
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Executes a query and streams rows through a mapping function. The connection stays open until enumeration ends.
        /// </summary>
        /// <typeparam name="TResult">Row type.</typeparam>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="map">Row mapper receiving the reader positioned on a row. Must not be null.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The mapped rows, streamed.</returns>
        public IEnumerable<TResult> Query<TResult>(SqlStatement statement, ITransaction? transaction, string operation, Func<DbDataReader, TResult> map, CommandType commandType = CommandType.Text)
        {
            return QueryCore(null, true, transaction, statement, operation, map, commandType);
        }

        /// <summary>
        /// Executes a query on an existing lease and streams rows through a mapping function.
        /// </summary>
        /// <typeparam name="TResult">Row type.</typeparam>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="map">Row mapper. Must not be null.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The mapped rows, streamed.</returns>
        public IEnumerable<TResult> Query<TResult>(ConnectionLease lease, SqlStatement statement, string operation, Func<DbDataReader, TResult> map, CommandType commandType = CommandType.Text)
        {
            return QueryCore(lease, false, null, statement, operation, map, commandType);
        }

        /// <summary>
        /// Executes a query and streams rows asynchronously through a mapping function.
        /// </summary>
        /// <typeparam name="TResult">Row type.</typeparam>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="map">Row mapper. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The mapped rows, streamed.</returns>
        public IAsyncEnumerable<TResult> QueryAsync<TResult>(SqlStatement statement, ITransaction? transaction, string operation, Func<DbDataReader, TResult> map, CancellationToken token = default, CommandType commandType = CommandType.Text)
        {
            return QueryCoreAsync(null, true, transaction, statement, operation, map, commandType, token);
        }

        /// <summary>
        /// Executes a query on an existing lease and streams rows asynchronously.
        /// </summary>
        /// <typeparam name="TResult">Row type.</typeparam>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="map">Row mapper. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The mapped rows, streamed.</returns>
        public IAsyncEnumerable<TResult> QueryAsync<TResult>(ConnectionLease lease, SqlStatement statement, string operation, Func<DbDataReader, TResult> map, CancellationToken token = default, CommandType commandType = CommandType.Text)
        {
            return QueryCoreAsync(lease, false, null, statement, operation, map, commandType, token);
        }

        /// <summary>
        /// Executes a command and hands the open reader to a callback (for multiple result sets).
        /// </summary>
        /// <typeparam name="TResult">Callback result type.</typeparam>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="consume">Callback consuming the reader. Must not be null.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The callback result.</returns>
        public TResult ExecuteReader<TResult>(SqlStatement statement, ITransaction? transaction, string operation, Func<DbDataReader, TResult> consume, CommandType commandType = CommandType.Text)
        {
            using ConnectionLease lease = Lease(transaction);
            return ExecuteReader(lease, statement, operation, consume, commandType);
        }

        /// <summary>
        /// Executes a command on a lease and hands the open reader to a callback.
        /// </summary>
        /// <typeparam name="TResult">Callback result type.</typeparam>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="consume">Callback consuming the reader. Must not be null.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The callback result.</returns>
        public TResult ExecuteReader<TResult>(ConnectionLease lease, SqlStatement statement, string operation, Func<DbDataReader, TResult> consume, CommandType commandType = CommandType.Text)
        {
            using DbCommand command = CreateCommand(lease, statement);
            command.CommandType = commandType;
            CommandScope scope = Begin(command, statement, operation);
            try
            {
                TResult result;
                using (DbDataReader reader = command.ExecuteReader())
                {
                    result = consume(reader);
                }

                CopyOutputParameters(command, statement);
                scope.Complete(null);
                return result;
            }
            catch (Exception e)
            {
                scope.Fail(e);
                throw;
            }
        }

        /// <summary>
        /// Executes a command and hands the open reader to an asynchronous callback.
        /// </summary>
        /// <typeparam name="TResult">Callback result type.</typeparam>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="transaction">Transaction; may be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="consume">Callback consuming the reader. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The callback result.</returns>
        public async Task<TResult> ExecuteReaderAsync<TResult>(SqlStatement statement, ITransaction? transaction, string operation, Func<DbDataReader, CancellationToken, Task<TResult>> consume, CancellationToken token, CommandType commandType = CommandType.Text)
        {
            ConnectionLease lease = await LeaseAsync(transaction, token).ConfigureAwait(false);
            await using (lease.ConfigureAwait(false))
            {
                return await ExecuteReaderAsync(lease, statement, operation, consume, token, commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Executes a command on a lease and hands the open reader to an asynchronous callback.
        /// </summary>
        /// <typeparam name="TResult">Callback result type.</typeparam>
        /// <param name="lease">Lease. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="operation">Operation name for diagnostics.</param>
        /// <param name="consume">Callback consuming the reader. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="commandType">Command type. Default: text.</param>
        /// <returns>The callback result.</returns>
        public async Task<TResult> ExecuteReaderAsync<TResult>(ConnectionLease lease, SqlStatement statement, string operation, Func<DbDataReader, CancellationToken, Task<TResult>> consume, CancellationToken token, CommandType commandType = CommandType.Text)
        {
            DbCommand command = CreateCommand(lease, statement);
            await using (command.ConfigureAwait(false))
            {
                command.CommandType = commandType;
                CommandScope scope = Begin(command, statement, operation);
                try
                {
                    TResult result;
                    DbDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                    await using (reader.ConfigureAwait(false))
                    {
                        result = await consume(reader, token).ConfigureAwait(false);
                    }

                    CopyOutputParameters(command, statement);
                    scope.Complete(null);
                    return result;
                }
                catch (Exception e)
                {
                    scope.Fail(e);
                    throw;
                }
            }
        }

        /// <summary>
        /// Executes a command returning several result sets and returns a reader over them. Diagnostics complete when the
        /// returned reader is disposed.
        /// </summary>
        /// <param name="lease">Lease, owned by the returned reader. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="converter">Converter for mapping rows. Must not be null.</param>
        /// <returns>The reader.</returns>
        public SqlMultipleResultReader ExecuteMultiple(ConnectionLease lease, SqlStatement statement, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(lease);
            ArgumentNullException.ThrowIfNull(converter);
            DbCommand command = CreateCommand(lease, statement);
            CommandScope scope = Begin(command, statement, "RAW");
            try
            {
                DbDataReader reader = command.ExecuteReader();
                return new SqlMultipleResultReader(lease, command, reader, converter, () => scope.Complete(null));
            }
            catch (Exception e)
            {
                scope.Fail(e);
                command.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Executes a command returning several result sets and returns a reader over them.
        /// </summary>
        /// <param name="lease">Lease, owned by the returned reader. Must not be null.</param>
        /// <param name="statement">Statement. Must not be null.</param>
        /// <param name="converter">Converter for mapping rows. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The reader.</returns>
        public async Task<SqlMultipleResultReader> ExecuteMultipleAsync(ConnectionLease lease, SqlStatement statement, IDataTypeConverter converter, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(lease);
            ArgumentNullException.ThrowIfNull(converter);
            DbCommand command = CreateCommand(lease, statement);
            CommandScope scope = Begin(command, statement, "RAW");
            try
            {
                DbDataReader reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                return new SqlMultipleResultReader(lease, command, reader, converter, () => scope.Complete(null));
            }
            catch (Exception e)
            {
                scope.Fail(e);
                await command.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        #endregion

        #region Private-Methods

        private IEnumerable<TResult> QueryCore<TResult>(ConnectionLease? lease, bool acquireLease, ITransaction? transaction, SqlStatement statement, string operation, Func<DbDataReader, TResult> map, CommandType commandType)
        {
            ConnectionLease? owned = acquireLease ? Lease(transaction) : null;
            try
            {
                using DbCommand command = CreateCommand(owned ?? lease!, statement);
                command.CommandType = commandType;
                CommandScope scope = Begin(command, statement, operation);
                DbDataReader reader;
                try
                {
                    reader = command.ExecuteReader();
                }
                catch (Exception e)
                {
                    scope.Fail(e);
                    throw;
                }

                long rows = 0;
                try
                {
                    using (reader)
                    {
                        while (true)
                        {
                            TResult item;
                            try
                            {
                                if (!reader.Read()) break;
                                item = map(reader);
                                rows++;
                            }
                            catch (Exception e)
                            {
                                scope.Fail(e);
                                throw;
                            }

                            yield return item;
                        }
                    }
                }
                finally
                {
                    scope.Complete(rows);
                }
            }
            finally
            {
                owned?.Dispose();
            }
        }

        private async IAsyncEnumerable<TResult> QueryCoreAsync<TResult>(ConnectionLease? lease, bool acquireLease, ITransaction? transaction, SqlStatement statement, string operation, Func<DbDataReader, TResult> map, CommandType commandType, [EnumeratorCancellation] CancellationToken token)
        {
            ConnectionLease? owned = acquireLease ? await LeaseAsync(transaction, token).ConfigureAwait(false) : null;
            try
            {
                DbCommand command = CreateCommand(owned ?? lease!, statement);
                await using (command.ConfigureAwait(false))
                {
                    command.CommandType = commandType;
                    CommandScope scope = Begin(command, statement, operation);
                    DbDataReader reader;
                    try
                    {
                        reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        scope.Fail(e);
                        throw;
                    }

                    long rows = 0;
                    try
                    {
                        await using (reader.ConfigureAwait(false))
                        {
                            while (true)
                            {
                                TResult item;
                                try
                                {
                                    token.ThrowIfCancellationRequested();
                                    if (!await reader.ReadAsync(token).ConfigureAwait(false)) break;
                                    item = map(reader);
                                    rows++;
                                }
                                catch (Exception e)
                                {
                                    scope.Fail(e);
                                    throw;
                                }

                                yield return item;
                            }
                        }
                    }
                    finally
                    {
                        scope.Complete(rows);
                    }
                }
            }
            finally
            {
                if (owned != null) await owned.DisposeAsync().ConfigureAwait(false);
            }
        }

        private static void CopyOutputParameters(DbCommand command, SqlStatement statement)
        {
            foreach (SqlParameterValue value in statement.Parameters)
            {
                if (value.Direction == ParameterDirection.Input) continue;
                DbParameter parameter = command.Parameters[value.Name];
                value.Value = parameter.Value == DBNull.Value ? null : parameter.Value;
            }
        }

        private CommandScope Begin(DbCommand command, SqlStatement statement, string operation)
        {
            SqlCommandContext? context = null;
            if (Options.Interceptors.Count > 0)
            {
                context = new SqlCommandContext(command, operation, _EntityType, _TableName);
                foreach (ISqlCommandInterceptor interceptor in Options.Interceptors) interceptor.CommandExecuting(context);
            }

            _OnExecuted(statement);

            Activity? activity = DurableDiagnostics.ActivitySource.HasListeners()
                ? DurableDiagnostics.ActivitySource.StartActivity(operation + " " + _TableName, ActivityKind.Client)
                : null;
            if (activity != null)
            {
                activity.SetTag("db.system", Dialect.DbSystemName);
                activity.SetTag("db.collection.name", _TableName);
                activity.SetTag("db.operation.name", operation);
                activity.SetTag("db.query.text", command.CommandText);
                if (Options.LogParameterValues)
                {
                    foreach (SqlParameterValue parameter in statement.Parameters)
                        activity.SetTag("db.query.parameter." + parameter.Name.TrimStart('@', ':', '$'), parameter.Value?.ToString());
                }
            }

            return new CommandScope(this, command, statement, operation, context, activity);
        }

        internal void OnComplete(CommandScope scope, long? rows)
        {
            TimeSpan elapsed = scope.Elapsed;
            if (scope.Context != null)
            {
                foreach (ISqlCommandInterceptor interceptor in Options.Interceptors) interceptor.CommandExecuted(scope.Context, elapsed, rows);
            }

            if (scope.Activity != null)
            {
                if (rows.HasValue) scope.Activity.SetTag("db.response.returned_rows", rows.Value);
                scope.Activity.Dispose();
            }

            ILogger? logger = Options.Logger;
            if (logger == null) return;
            if (Options.SlowCommandThreshold.HasValue && elapsed >= Options.SlowCommandThreshold.Value && logger.IsEnabled(LogLevel.Warning))
            {
                logger.LogWarning("Slow {Operation} on {Table} took {ElapsedMs:F1} ms: {Sql}", scope.Operation, _TableName, elapsed.TotalMilliseconds, FormatForLog(scope));
            }
            else if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Executed {Operation} on {Table} in {ElapsedMs:F1} ms ({Rows} rows): {Sql}", scope.Operation, _TableName, elapsed.TotalMilliseconds, rows, FormatForLog(scope));
            }
        }

        internal void OnFail(CommandScope scope, Exception exception)
        {
            TimeSpan elapsed = scope.Elapsed;
            if (scope.Context != null)
            {
                foreach (ISqlCommandInterceptor interceptor in Options.Interceptors) interceptor.CommandFailed(scope.Context, exception, elapsed);
            }

            if (scope.Activity != null)
            {
                scope.Activity.SetStatus(ActivityStatusCode.Error, exception.Message);
                scope.Activity.SetTag("error.type", exception.GetType().FullName);
                scope.Activity.Dispose();
            }

            Options.Logger?.LogError(exception, "Failed {Operation} on {Table} after {ElapsedMs:F1} ms: {Sql}", scope.Operation, _TableName, elapsed.TotalMilliseconds, FormatForLog(scope));
        }

        private string FormatForLog(CommandScope scope)
        {
            return Options.LogParameterValues ? scope.Statement.ToDebugString() : scope.Command.CommandText;
        }

        #endregion
    }
}
