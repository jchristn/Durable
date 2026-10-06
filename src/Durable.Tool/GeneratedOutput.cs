namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Where generated source files go and which namespace they use: --output-dir relative to the project directory (or
    /// the working directory when there is no project), and --namespace or the project's root namespace followed by the
    /// output directory segments.
    /// </summary>
    internal sealed class GeneratedOutput
    {
        /// <summary>Gets the absolute output directory.</summary>
        public string Directory { get; }

        /// <summary>Gets the namespace of generated code.</summary>
        public string Namespace { get; }

        private GeneratedOutput(string directory, string namespaceName)
        {
            Directory = directory;
            Namespace = namespaceName;
        }

        /// <summary>
        /// Resolves the output directory and namespace for a command.
        /// </summary>
        /// <param name="invocation">Invocation. Must not be null.</param>
        /// <param name="outputDirOption">The command's --output-dir option. Must not be null.</param>
        /// <param name="defaultDirectory">Default output directory name. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The output location.</returns>
        /// <exception cref="DurableCliException">Thrown when the namespace is not a valid C# namespace.</exception>
        public static async Task<GeneratedOutput> ResolveAsync(CommandInvocation invocation, OptionDefinition outputDirOption, string defaultDirectory, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(invocation);
            ProjectInfo? project = await invocation.TryGetProjectForOutputAsync(token).ConfigureAwait(false);
            string baseDirectory = project?.Directory ?? invocation.Context.WorkingDirectory;
            string directory = Path.GetFullPath(Path.Combine(baseDirectory, invocation.Arguments.GetValue(outputDirOption) ?? defaultDirectory));

            string? namespaceName = invocation.Arguments.GetValue(CliOptions.Namespace);
            if (namespaceName == null)
            {
                string relative = Path.GetRelativePath(baseDirectory, directory);
                IEnumerable<string> segments = relative.StartsWith("..", StringComparison.Ordinal) || Path.IsPathRooted(relative)
                    ? new[] { Path.GetFileName(directory) }
                    : relative.Split(new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries).Where(s => s != ".");
                List<string> parts = new List<string>();
                if (project != null) parts.Add(project.RootNamespace);
                parts.AddRange(segments.Select(s => CSharpNames.ToPascalCase(s, "N")));
                namespaceName = parts.Count > 0 ? string.Join(".", parts) : CSharpNames.ToPascalCase(defaultDirectory, "N");
            }

            if (!CSharpNames.IsValidNamespace(namespaceName))
                throw new DurableCliException("'" + namespaceName + "' is not a valid C# namespace. Pass --namespace <ns>.", null, true);
            return new GeneratedOutput(directory, namespaceName);
        }
    }
}
