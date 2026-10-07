namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;

    /// <summary>
    /// The <c>durable</c> command-line tool as a library: parses arguments, runs the command and returns the exit code.
    /// <c>Program.Main</c> is a one-line call to <see cref="RunAsync(string[], TextWriter, TextWriter, CancellationToken)"/>,
    /// and tests or other hosts can call it in-process. Output goes to the supplied writers only.
    /// Thread safety: each call is independent; concurrent calls are safe when they use different writers.
    /// </summary>
    public static class DurableCli
    {
        /// <summary>
        /// Gets the tool version, for example "0.7.1".
        /// </summary>
        public static string Version { get; } = ReadVersion();

        /// <summary>
        /// Runs the tool in the process working directory with the process environment variables.
        /// </summary>
        /// <param name="args">Command-line arguments (without the program name). Must not be null.</param>
        /// <param name="output">Stream for command output. Must not be null.</param>
        /// <param name="error">Stream for errors, warnings and progress. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The exit code: <see cref="ExitCodes.Success"/>, <see cref="ExitCodes.CommandError"/> or <see cref="ExitCodes.UnexpectedFailure"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static Task<int> RunAsync(string[] args, TextWriter output, TextWriter error, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(args);
            return RunAsync(args, new DurableCliContext(output, error), token);
        }

        /// <summary>
        /// Runs the tool in an explicit context (working directory, environment variables and streams).
        /// </summary>
        /// <param name="args">Command-line arguments (without the program name). Must not be null.</param>
        /// <param name="context">Invocation context. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The exit code: <see cref="ExitCodes.Success"/>, <see cref="ExitCodes.CommandError"/> or <see cref="ExitCodes.UnexpectedFailure"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static async Task<int> RunAsync(string[] args, DurableCliContext context, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(args);
            ArgumentNullException.ThrowIfNull(context);
            bool verbose = args.Contains("--verbose", StringComparer.Ordinal);
            CommandDefinition? command = null;
            try
            {
                List<string> words = CommandLineParser.FindWords(args);
                bool help = args.Any(a => a == "--help" || a == "-h");
                if (words.Count == 0)
                {
                    if (args.Contains("--version", StringComparer.Ordinal))
                    {
                        context.Output.WriteLine(Version);
                        return ExitCodes.Success;
                    }

                    HelpWriter.WriteOverview(help ? context.Output : context.Error);
                    return help ? ExitCodes.Success : ExitCodes.CommandError;
                }

                if (string.Equals(words[0], "help", StringComparison.OrdinalIgnoreCase)) return WriteHelp(words.Skip(1).ToList(), context);

                command = CommandCatalog.Find(words);
                if (command == null)
                {
                    string first = words[0].ToLowerInvariant();
                    if (CommandCatalog.Groups.Contains(first) && (words.Count == 1 || help))
                    {
                        if (help)
                        {
                            HelpWriter.WriteGroup(context.Output, first);
                            return ExitCodes.Success;
                        }

                        context.Error.WriteLine("error: 'durable " + first + "' needs a subcommand.");
                        HelpWriter.WriteGroup(context.Error, first);
                        return ExitCodes.CommandError;
                    }

                    throw UnknownCommand(words);
                }

                if (help)
                {
                    HelpWriter.WriteCommand(context.Output, command);
                    return ExitCodes.Success;
                }

                ParsedArguments parsed = CommandLineParser.Parse(args, command.WordCount, command.AllOptions, command.Name);
                if (parsed.Positionals.Count < command.ArgumentCount)
                    throw new DurableCliException("'durable " + command.Name + "' requires " + command.ArgumentSyntax + ".", null, true);
                if (parsed.Positionals.Count > command.ArgumentCount)
                    throw new DurableCliException("Unexpected argument '" + parsed.Positionals[command.ArgumentCount] + "' for 'durable " + command.Name + "'.", null, true);

                ToolSettings settings = ToolSettings.Resolve(parsed, context);
                CommandInvocation invocation = new CommandInvocation(command, parsed, settings, context);
                return await command.Handler(invocation, token).ConfigureAwait(false);
            }
            catch (DurableCliException e)
            {
                context.Error.WriteLine("error: " + e.Message);
                if (!string.IsNullOrWhiteSpace(e.Detail)) context.Error.WriteLine(e.Detail.TrimEnd());
                if (e.SuggestHelp) context.Error.WriteLine("Run 'durable " + (command != null ? command.Name + " " : string.Empty) + "--help' for usage.");
                return ExitCodes.CommandError;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                context.Error.WriteLine("error: cancelled.");
                return ExitCodes.CommandError;
            }
            catch (Exception e) when (DuckDbNativeLibrary.IsMissingLibrary(e))
            {
                return ReportCommandError(context, DuckDbNativeLibrary.MissingMessage, e, verbose);
            }
            catch (MigrationException e)
            {
                return ReportCommandError(context, e.Message, e, verbose);
            }
            catch (DbException e)
            {
                return ReportCommandError(context, "database error: " + e.Message, e, verbose);
            }
            catch (Exception e) when (e is FileNotFoundException || e is FileLoadException || e is TypeLoadException)
            {
                return ReportCommandError(context, "could not load your assembly or one of its dependencies: " + e.Message, e, verbose);
            }
            catch (Exception e) when (e is InvalidOperationException || e is ArgumentException || e is NotSupportedException)
            {
                return ReportCommandError(context, e.Message, e, verbose);
            }
            catch (Exception e)
            {
                context.Error.WriteLine("error: unexpected failure in durable " + Version + ": " + e);
                return ExitCodes.UnexpectedFailure;
            }
        }

        private static int WriteHelp(List<string> words, DurableCliContext context)
        {
            if (words.Count == 0)
            {
                HelpWriter.WriteOverview(context.Output);
                return ExitCodes.Success;
            }

            CommandDefinition? command = CommandCatalog.Find(words);
            if (command != null)
            {
                HelpWriter.WriteCommand(context.Output, command);
                return ExitCodes.Success;
            }

            string first = words[0].ToLowerInvariant();
            if (CommandCatalog.Groups.Contains(first))
            {
                HelpWriter.WriteGroup(context.Output, first);
                return ExitCodes.Success;
            }

            DurableCliException error = UnknownCommand(words);
            context.Error.WriteLine("error: " + error.Message);
            context.Error.WriteLine("Run 'durable --help' for usage.");
            return ExitCodes.CommandError;
        }

        private static DurableCliException UnknownCommand(List<string> words)
        {
            string typed = words.Count > 1 && CommandCatalog.Groups.Contains(words[0].ToLowerInvariant()) ? words[0] + " " + words[1] : words[0];
            List<string> names = CommandCatalog.Commands.Select(c => c.Name).Concat(CommandCatalog.Groups).Distinct().ToList();
            string? suggestion = CommandLineParser.Suggest(typed, names);
            return new DurableCliException("Unknown command '" + typed + "'." + (suggestion != null ? " Did you mean '" + suggestion + "'?" : string.Empty), null, true);
        }

        private static int ReportCommandError(DurableCliContext context, string message, Exception exception, bool verbose)
        {
            context.Error.WriteLine("error: " + message);
            if (exception.InnerException != null && !message.Contains(exception.InnerException.Message, StringComparison.Ordinal))
                context.Error.WriteLine("  caused by: " + exception.InnerException.Message);
            if (verbose) context.Error.WriteLine(exception.ToString());
            return ExitCodes.CommandError;
        }

        private static string ReadVersion()
        {
            Assembly assembly = typeof(DurableCli).Assembly;
            string? informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
            if (!string.IsNullOrEmpty(informational))
            {
                int plus = informational.IndexOf('+', StringComparison.Ordinal);
                return plus >= 0 ? informational.Substring(0, plus) : informational;
            }

            return assembly.GetName().Version?.ToString(3) ?? "0.0.0";
        }
    }
}
