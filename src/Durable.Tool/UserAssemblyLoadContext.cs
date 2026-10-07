namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Runtime.Loader;

    /// <summary>
    /// Loads the user's assembly and its dependencies in isolation from the tool, resolving them with the assembly's
    /// <c>.deps.json</c> (or its directory). The Durable assemblies are deliberately not loaded here: they resolve from the
    /// tool's own context so <c>Migration</c>, <c>[Entity]</c> and the other Durable types keep a single identity.
    /// </summary>
    internal sealed class UserAssemblyLoadContext : AssemblyLoadContext
    {
        /// <summary>
        /// Gets the names of the assemblies shared with the tool.
        /// </summary>
        public static IReadOnlySet<string> SharedAssemblyNames { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Durable", "Durable.Sql", "Durable.Sqlite", "Durable.DuckDb", "Durable.Postgres", "Durable.MySql", "Durable.SqlServer", "Durable.Oracle"
        };

        private readonly AssemblyDependencyResolver? _Resolver;
        private readonly string _Directory;

        /// <summary>
        /// Instantiates a context for an assembly.
        /// </summary>
        /// <param name="assemblyPath">Absolute path of the user's assembly. Must not be null.</param>
        public UserAssemblyLoadContext(string assemblyPath) : base("durable-tool:" + Path.GetFileName(assemblyPath), false)
        {
            _Directory = Path.GetDirectoryName(assemblyPath) ?? ".";
            try
            {
                _Resolver = new AssemblyDependencyResolver(assemblyPath);
            }
            catch (InvalidOperationException)
            {
                _Resolver = null;
            }
        }

        /// <inheritdoc />
        protected override Assembly? Load(AssemblyName assemblyName)
        {
            if (assemblyName.Name == null || SharedAssemblyNames.Contains(assemblyName.Name)) return null;
            string? path = _Resolver?.ResolveAssemblyToPath(assemblyName);
            if (path == null)
            {
                string candidate = Path.Combine(_Directory, assemblyName.Name + ".dll");
                if (File.Exists(candidate)) path = candidate;
            }

            return path != null ? LoadFromAssemblyPath(path) : null;
        }

        /// <inheritdoc />
        protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
        {
            string? path = _Resolver?.ResolveUnmanagedDllToPath(unmanagedDllName);
            return path != null ? LoadUnmanagedDllFromPath(path) : IntPtr.Zero;
        }
    }
}
