namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Runs child processes (the dotnet CLI) and captures their output.
    /// </summary>
    internal static class ProcessRunner
    {
        /// <summary>
        /// Runs a process to completion.
        /// </summary>
        /// <param name="fileName">Executable. Must not be null.</param>
        /// <param name="arguments">Arguments, passed without shell interpretation. Must not be null.</param>
        /// <param name="workingDirectory">Working directory. Must not be null.</param>
        /// <param name="token">Cancellation token; cancelling kills the process tree.</param>
        /// <returns>The result.</returns>
        /// <exception cref="DurableCliException">Thrown when the executable cannot be started.</exception>
        public static async Task<ProcessResult> RunAsync(string fileName, IEnumerable<string> arguments, string workingDirectory, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(fileName);
            ArgumentNullException.ThrowIfNull(arguments);
            ProcessStartInfo info = new ProcessStartInfo(fileName)
            {
                WorkingDirectory = workingDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            foreach (string argument in arguments) info.ArgumentList.Add(argument);
            info.Environment["DOTNET_CLI_UI_LANGUAGE"] = "en";
            info.Environment["MSBUILDTERMINALLOGGER"] = "off";
            info.Environment["DOTNET_NOLOGO"] = "1";

            using Process process = new Process { StartInfo = info };
            try
            {
                process.Start();
            }
            catch (Win32Exception e)
            {
                throw new DurableCliException("Could not start '" + fileName + "': " + e.Message + ". Is the .NET SDK installed and on the PATH?");
            }

            Task<string> output = process.StandardOutput.ReadToEndAsync(token);
            Task<string> error = process.StandardError.ReadToEndAsync(token);
            try
            {
                await process.WaitForExitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                try
                {
                    process.Kill(true);
                }
                catch (InvalidOperationException)
                {
                }

                throw;
            }

            return new ProcessResult(process.ExitCode, await output.ConfigureAwait(false), await error.ConfigureAwait(false));
        }
    }
}
