namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;

    /// <summary>
    /// Tracks one command execution for interceptors, tracing and logging. Internal to <see cref="SqlCommandExecutor"/>.
    /// For drivers that ignore <see cref="DbCommand.CommandTimeout"/> (<see cref="ISqlDialect.DriverEnforcesCommandTimeout"/>),
    /// it also enforces the configured timeout by canceling the command.
    /// </summary>
    internal sealed class CommandScope
    {
        private readonly SqlCommandExecutor _Executor;
        private readonly long _StartTimestamp;
        private bool _Finished;
        private Timer? _TimeoutTimer;
        private TimeSpan _Timeout;
        private int _TimedOut;

        internal CommandScope(SqlCommandExecutor executor, DbCommand command, SqlStatement statement, string operation, SqlCommandContext? context, Activity? activity)
        {
            _Executor = executor;
            Command = command;
            Statement = statement;
            Operation = operation;
            Context = context;
            Activity = activity;
            _StartTimestamp = Stopwatch.GetTimestamp();
        }

        internal DbCommand Command { get; }

        internal SqlStatement Statement { get; }

        internal string Operation { get; }

        internal SqlCommandContext? Context { get; }

        internal Activity? Activity { get; }

        internal TimeSpan Elapsed { get; private set; }

        internal void StartTimeout(TimeSpan timeout)
        {
            _Timeout = timeout;
            _TimeoutTimer = new Timer(OnTimeout, null, timeout, Timeout.InfiniteTimeSpan);
        }

        internal void Complete(long? rows)
        {
            if (_Finished) return;
            _Finished = true;
            StopTimeout();
            Elapsed = Stopwatch.GetElapsedTime(_StartTimestamp);
            _Executor.OnComplete(this, rows);
        }

        internal void Fail(Exception exception)
        {
            if (_Finished) return;
            _Finished = true;
            StopTimeout();
            Elapsed = Stopwatch.GetElapsedTime(_StartTimestamp);
            _Executor.OnFail(this, exception);
        }

        internal void ThrowIfTimedOut(Exception exception)
        {
            if (Volatile.Read(ref _TimedOut) == 1)
            {
                throw new TimeoutException(
                    "The command was canceled after the command timeout of " + _Timeout.TotalSeconds.ToString(CultureInfo.InvariantCulture) + " s elapsed.",
                    exception);
            }
        }

        private void OnTimeout(object? state)
        {
            if (Volatile.Read(ref _TimedOut) == 1 || _Finished) return;
            Interlocked.Exchange(ref _TimedOut, 1);
            try
            {
                Command.Cancel();
            }
            catch (Exception)
            {
                // Cancel is best effort: the command may have completed or been disposed in the meantime.
            }
        }

        private void StopTimeout()
        {
            Timer? timer = Interlocked.Exchange(ref _TimeoutTimer, null);
            timer?.Dispose();
        }
    }
}
