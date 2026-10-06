namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Locates, inspects and builds the user's project with the dotnet CLI.
    /// </summary>
    internal static class ProjectBuilder
    {
        /// <summary>
        /// Finds the project file: the given file, the single project file in the given directory, or the single project
        /// file in the working directory.
        /// </summary>
        /// <param name="projectPath">Project file or directory from the settings, or null.</param>
        /// <param name="context">Invocation context. Must not be null.</param>
        /// <param name="required">Whether a missing project is an error (otherwise null is returned).</param>
        /// <returns>The project file path, or null when none was found and it is not required.</returns>
        /// <exception cref="DurableCliException">Thrown when the project is missing (and required) or ambiguous.</exception>
        public static string? Locate(string? projectPath, DurableCliContext context, bool required)
        {
            ArgumentNullException.ThrowIfNull(context);
            string directory = context.WorkingDirectory;
            if (projectPath != null)
            {
                if (File.Exists(projectPath)) return projectPath;
                if (!Directory.Exists(projectPath)) throw new DurableCliException("Project '" + projectPath + "' was not found.");
                directory = projectPath;
            }

            List<string> projects = Directory.GetFiles(directory, "*.csproj").OrderBy(p => p, StringComparer.Ordinal).ToList();
            if (projects.Count == 1) return projects[0];
            if (projects.Count > 1)
                throw new DurableCliException("Found " + projects.Count + " projects in '" + directory + "' (" + string.Join(", ", projects.Select(Path.GetFileName)) + "). Pick one with --project.", null, true);
            if (projectPath != null || required)
                throw new DurableCliException(
                    "No project file (.csproj) found in '" + directory + "'. Run the command from your project directory, or pass --project <path> or --assembly <path to built .dll>.",
                    null, true);
            return null;
        }

        /// <summary>
        /// Reads the project's root namespace and target path for a configuration and framework.
        /// </summary>
        /// <param name="projectFile">Project file. Must not be null.</param>
        /// <param name="configuration">Build configuration. Must not be null.</param>
        /// <param name="framework">Target framework, or null.</param>
        /// <param name="context">Invocation context. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The project information.</returns>
        /// <exception cref="DurableCliException">Thrown when the project cannot be evaluated or multi-targets without --framework.</exception>
        public static async Task<ProjectInfo> InspectAsync(string projectFile, string configuration, string? framework, DurableCliContext context, CancellationToken token)
        {
            Dictionary<string, string> properties = await QueryAsync(projectFile, configuration, framework, context, token).ConfigureAwait(false);
            string targetPath = Get(properties, "TargetPath");
            if (targetPath.Length == 0)
            {
                List<string> frameworks = ToolSettings.SplitList(Get(properties, "TargetFrameworks").Replace(';', ','));
                if (framework == null && frameworks.Count == 1) return await InspectAsync(projectFile, configuration, frameworks[0], context, token).ConfigureAwait(false);
                if (framework == null && frameworks.Count > 1)
                    throw new DurableCliException("Project '" + Path.GetFileName(projectFile) + "' targets several frameworks (" + string.Join(", ", frameworks) + "). Pick one with --framework.", null, true);
                throw new DurableCliException("Could not determine the output assembly of '" + projectFile + "'. Pass --assembly <path to built .dll>.");
            }

            string rootNamespace = Get(properties, "RootNamespace");
            if (rootNamespace.Length == 0) rootNamespace = Path.GetFileNameWithoutExtension(projectFile);
            string? selected = framework ?? NullIfEmpty(Get(properties, "TargetFramework"));
            return new ProjectInfo(projectFile, rootNamespace, Path.GetFullPath(targetPath, Path.GetDirectoryName(projectFile) ?? context.WorkingDirectory), selected);
        }

        /// <summary>
        /// Builds the project with <c>dotnet build</c>.
        /// </summary>
        /// <param name="project">Project information. Must not be null.</param>
        /// <param name="configuration">Build configuration. Must not be null.</param>
        /// <param name="context">Invocation context. Must not be null.</param>
        /// <param name="verbose">Whether to echo the build output.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="DurableCliException">Thrown when the build fails.</exception>
        public static async Task BuildAsync(ProjectInfo project, string configuration, DurableCliContext context, bool verbose, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(project);
            List<string> arguments = new List<string> { "build", project.ProjectPath, "-c", configuration, "-nologo", "-v", "q" };
            if (project.Framework != null)
            {
                arguments.Add("-f");
                arguments.Add(project.Framework);
            }

            context.Error.WriteLine("Building " + Path.GetFileName(project.ProjectPath) + " (" + configuration + (project.Framework != null ? ", " + project.Framework : string.Empty) + ")...");
            ProcessResult result = await ProcessRunner.RunAsync(DotnetPath(context), arguments, project.Directory, token).ConfigureAwait(false);
            if (verbose) context.Error.WriteLine(result.CombinedOutput);
            if (result.ExitCode != 0)
                throw new DurableCliException("Build failed for '" + project.ProjectPath + "' (exit code " + result.ExitCode + ").", result.CombinedOutput);
        }

        private static async Task<Dictionary<string, string>> QueryAsync(string projectFile, string configuration, string? framework, DurableCliContext context, CancellationToken token)
        {
            List<string> arguments = new List<string>
            {
                "msbuild", projectFile, "-nologo",
                "-getProperty:TargetPath", "-getProperty:TargetFramework", "-getProperty:TargetFrameworks", "-getProperty:RootNamespace",
                "-p:Configuration=" + configuration
            };
            if (framework != null) arguments.Add("-p:TargetFramework=" + framework);
            ProcessResult result = await ProcessRunner.RunAsync(DotnetPath(context), arguments, Path.GetDirectoryName(projectFile) ?? context.WorkingDirectory, token).ConfigureAwait(false);
            if (result.ExitCode != 0)
                throw new DurableCliException("Could not evaluate project '" + projectFile + "' (dotnet msbuild exit code " + result.ExitCode + ").", result.CombinedOutput);

            Dictionary<string, string> properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using JsonDocument document = JsonDocument.Parse(result.StandardOutput);
                if (document.RootElement.TryGetProperty("Properties", out JsonElement values))
                {
                    foreach (JsonProperty property in values.EnumerateObject())
                        properties[property.Name] = property.Value.GetString() ?? string.Empty;
                }
            }
            catch (JsonException e)
            {
                throw new DurableCliException("Could not read the properties of project '" + projectFile + "': " + e.Message, result.CombinedOutput);
            }

            return properties;
        }

        private static string DotnetPath(DurableCliContext context)
        {
            string? host = context.GetEnvironmentVariable("DOTNET_HOST_PATH");
            return !string.IsNullOrWhiteSpace(host) && File.Exists(host) ? host : "dotnet";
        }

        private static string Get(Dictionary<string, string> properties, string name)
        {
            return properties.TryGetValue(name, out string? value) ? value.Trim() : string.Empty;
        }

        private static string? NullIfEmpty(string value)
        {
            return value.Length == 0 ? null : value;
        }
    }
}
