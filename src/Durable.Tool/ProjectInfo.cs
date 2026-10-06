namespace Durable.Tool
{
    using System.IO;

    /// <summary>
    /// MSBuild properties of the user's project, read with <c>dotnet msbuild -getProperty</c>.
    /// </summary>
    internal sealed class ProjectInfo
    {
        /// <summary>Gets the absolute project file path.</summary>
        public string ProjectPath { get; }

        /// <summary>Gets the project directory.</summary>
        public string Directory => Path.GetDirectoryName(ProjectPath) ?? ".";

        /// <summary>Gets the root namespace (falls back to the project file name).</summary>
        public string RootNamespace { get; }

        /// <summary>Gets the built assembly path for the selected configuration and framework.</summary>
        public string TargetPath { get; }

        /// <summary>Gets the selected target framework, or null for a single-targeted project queried without one.</summary>
        public string? Framework { get; }

        /// <summary>
        /// Instantiates project information.
        /// </summary>
        /// <param name="projectPath">Project file path. Must not be null.</param>
        /// <param name="rootNamespace">Root namespace. Must not be null.</param>
        /// <param name="targetPath">Built assembly path. Must not be null.</param>
        /// <param name="framework">Target framework, or null.</param>
        public ProjectInfo(string projectPath, string rootNamespace, string targetPath, string? framework)
        {
            ProjectPath = projectPath;
            RootNamespace = rootNamespace;
            TargetPath = targetPath;
            Framework = framework;
        }
    }
}
