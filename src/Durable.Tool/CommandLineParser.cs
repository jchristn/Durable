namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// A small GNU-style argument parser: <c>--name value</c>, <c>--name=value</c>, flags, <c>-h</c>, and <c>--</c> to end
    /// option processing. Options may appear before, between or after the command words and positional arguments.
    /// </summary>
    internal static class CommandLineParser
    {
        /// <summary>
        /// Returns the non-option tokens in order (command words followed by positional arguments), skipping the values of
        /// known value options. Used to identify the command before its option set is known.
        /// </summary>
        /// <param name="args">Arguments. Must not be null.</param>
        /// <returns>The words.</returns>
        public static List<string> FindWords(string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);
            List<string> words = new List<string>();
            bool optionsEnded = false;
            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                if (optionsEnded || !IsOptionToken(arg))
                {
                    words.Add(arg);
                    continue;
                }

                if (arg == "--")
                {
                    optionsEnded = true;
                    continue;
                }

                if (arg.StartsWith("--", StringComparison.Ordinal) && !arg.Contains('=', StringComparison.Ordinal))
                {
                    string name = arg.Substring(2);
                    OptionDefinition? option = CliOptions.All.FirstOrDefault(o => o.Name == name);
                    if (option != null && !option.IsFlag) i++;
                }
            }

            return words;
        }

        /// <summary>
        /// Parses the arguments of a command.
        /// </summary>
        /// <param name="args">Arguments. Must not be null.</param>
        /// <param name="commandWordCount">Number of leading words naming the command; they are not positional arguments.</param>
        /// <param name="options">Options the command accepts. Must not be null.</param>
        /// <param name="commandName">Command name for error messages. Must not be null.</param>
        /// <returns>The parsed arguments.</returns>
        /// <exception cref="DurableCliException">Thrown for unknown options, missing values or values given to flags.</exception>
        public static ParsedArguments Parse(string[] args, int commandWordCount, IReadOnlyList<OptionDefinition> options, string commandName)
        {
            ArgumentNullException.ThrowIfNull(args);
            ArgumentNullException.ThrowIfNull(options);
            ParsedArguments parsed = new ParsedArguments();
            int wordsToSkip = commandWordCount;
            bool optionsEnded = false;
            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                if (optionsEnded || !IsOptionToken(arg))
                {
                    if (wordsToSkip > 0) wordsToSkip--;
                    else parsed.Positionals.Add(arg);
                    continue;
                }

                if (arg == "--")
                {
                    optionsEnded = true;
                    continue;
                }

                string name;
                string? inlineValue = null;
                OptionDefinition? option;
                if (arg.StartsWith("--", StringComparison.Ordinal))
                {
                    name = arg.Substring(2);
                    int equals = name.IndexOf('=', StringComparison.Ordinal);
                    if (equals >= 0)
                    {
                        inlineValue = name.Substring(equals + 1);
                        name = name.Substring(0, equals);
                    }

                    option = options.FirstOrDefault(o => o.Name == name);
                }
                else
                {
                    name = arg.Substring(1);
                    option = name.Length == 1 ? options.FirstOrDefault(o => o.ShortName == name[0]) : null;
                }

                if (option == null) throw UnknownOption(arg, options, commandName);

                if (option.IsFlag)
                {
                    if (inlineValue != null) throw new DurableCliException("Option --" + option.Name + " does not take a value.", null, true);
                    parsed.SetFlag(option);
                    continue;
                }

                string? value = inlineValue;
                if (value == null)
                {
                    if (i + 1 >= args.Length || (IsOptionToken(args[i + 1]) && args[i + 1] != "-"))
                        throw new DurableCliException("Option --" + option.Name + " requires a value <" + option.ValueName + ">.", null, true);
                    value = args[++i];
                }

                parsed.SetValue(option, value);
            }

            return parsed;
        }

        /// <summary>
        /// Returns the candidate closest to a mistyped value, or null when none is close.
        /// </summary>
        /// <param name="value">Mistyped value. Must not be null.</param>
        /// <param name="candidates">Valid values. Must not be null.</param>
        /// <returns>The suggestion or null.</returns>
        public static string? Suggest(string value, IEnumerable<string> candidates)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(candidates);
            return candidates
                .OrderBy(c => Distance(c, value))
                .FirstOrDefault(c => Distance(c, value) <= Math.Max(2, value.Length / 3));
        }

        private static bool IsOptionToken(string arg)
        {
            return arg.Length > 1 && arg[0] == '-' && !char.IsDigit(arg[1]);
        }

        private static DurableCliException UnknownOption(string arg, IReadOnlyList<OptionDefinition> options, string commandName)
        {
            string name = arg.TrimStart('-');
            int equals = name.IndexOf('=', StringComparison.Ordinal);
            if (equals >= 0) name = name.Substring(0, equals);
            string? closest = Suggest(name, options.Select(o => o.Name));
            string message = "Unknown option '" + arg + "' for 'durable " + commandName + "'.";
            if (closest != null) message += " Did you mean --" + closest + "?";
            return new DurableCliException(message, null, true);
        }

        private static int Distance(string a, string b)
        {
            int[,] d = new int[a.Length + 1, b.Length + 1];
            for (int i = 0; i <= a.Length; i++) d[i, 0] = i;
            for (int j = 0; j <= b.Length; j++) d[0, j] = j;
            for (int i = 1; i <= a.Length; i++)
            {
                for (int j = 1; j <= b.Length; j++)
                {
                    int cost = a[i - 1] == b[j - 1] ? 0 : 1;
                    d[i, j] = Math.Min(Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1), d[i - 1, j - 1] + cost);
                }
            }

            return d[a.Length, b.Length];
        }
    }
}
