namespace Durable.Sql
{
    using System;
    using System.Data.Common;
    using System.Diagnostics;

    /// <summary>
    /// Tracks one command execution for interceptors, tracing and logging. Internal to <see cref="SqlCommandExecutor"/>.
    /// </summary>
    internal sealed class CommandScope
    {
        private readonly SqlCommandExecutor _Executor;
        private readonly long _StartTimestamp;
        private bool _Finished;

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

        internal void Complete(long? rows)
        {
            if (_Finished) return;
            _Finished = true;
            Elapsed = Stopwatch.GetElapsedTime(_StartTimestamp);
            _Executor.OnComplete(this, rows);
        }

        internal void Fail(Exception exception)
        {
            if (_Finished) return;
            _Finished = true;
            Elapsed = Stopwatch.GetElapsedTime(_StartTimestamp);
            _Executor.OnFail(this, exception);
        }
    }
}
