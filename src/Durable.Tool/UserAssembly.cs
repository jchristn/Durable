namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// The user's built assembly, loaded in a <see cref="UserAssemblyLoadContext"/>, with discovery of migrations and
    /// entity types. Loaded assemblies are cached per path and file timestamp for the lifetime of the process, so repeated
    /// in-process invocations reuse one load context. Thread-safe.
    /// </summary>
    internal sealed class UserAssembly
    {
        /// <summary>Gets the loaded assembly.</summary>
        public Assembly Assembly { get; }

        /// <summary>Gets the assembly path.</summary>
        public string Path { get; }

        /// <summary>Gets warnings found while loading (type load failures, Durable version mismatches).</summary>
        public IReadOnlyList<string> Warnings => _Warnings;

        private static readonly object _CacheLock = new object();
        private static readonly Dictionary<string, UserAssembly> _Cache = new Dictionary<string, UserAssembly>(StringComparer.Ordinal);
        private readonly List<string> _Warnings = new List<string>();
        private readonly List<Type> _Types;

        private UserAssembly(Assembly assembly, string path)
        {
            Assembly = assembly;
            Path = path;
            _Types = LoadTypes();
            CheckDurableVersions();
        }

        /// <summary>
        /// Loads (or returns the cached) assembly at a path.
        /// </summary>
        /// <param name="path">Absolute assembly path. Must not be null.</param>
        /// <returns>The loaded assembly.</returns>
        /// <exception cref="DurableCliException">Thrown when the file does not exist or is not a loadable .NET assembly.</exception>
        public static UserAssembly Load(string path)
        {
            ArgumentNullException.ThrowIfNull(path);
            if (!File.Exists(path)) throw new DurableCliException("Assembly '" + path + "' was not found. Build the project first or check --assembly.");
            string key = path + "|" + File.GetLastWriteTimeUtc(path).Ticks;
            lock (_CacheLock)
            {
                if (_Cache.TryGetValue(key, out UserAssembly? cached)) return cached;
                try
                {
                    UserAssemblyLoadContext context = new UserAssemblyLoadContext(path);
                    UserAssembly loaded = new UserAssembly(context.LoadFromAssemblyPath(path), path);
                    _Cache[key] = loaded;
                    return loaded;
                }
                catch (BadImageFormatException e)
                {
                    throw new DurableCliException("'" + path + "' is not a .NET assembly: " + e.Message);
                }
                catch (FileLoadException e)
                {
                    throw new DurableCliException("Assembly '" + path + "' could not be loaded: " + e.Message);
                }
            }
        }

        /// <summary>
        /// Instantiates the concrete migrations (public parameterless constructor) in the assembly.
        /// </summary>
        /// <param name="namespaceFilter">Namespace to restrict discovery to (including nested namespaces), or null for all.</param>
        /// <returns>The migrations, ordered by Id.</returns>
        /// <exception cref="DurableCliException">Thrown when a migration cannot be instantiated or two share an Id.</exception>
        public List<Migration> DiscoverMigrations(string? namespaceFilter)
        {
            List<Migration> migrations = new List<Migration>();
            foreach (Type type in _Types.Where(t => t.IsClass && !t.IsAbstract && !t.ContainsGenericParameters && typeof(Migration).IsAssignableFrom(t)))
            {
                if (!InNamespace(type, namespaceFilter) || type.GetConstructor(Type.EmptyTypes) == null) continue;
                try
                {
                    migrations.Add((Migration)Activator.CreateInstance(type)!);
                }
                catch (TargetInvocationException e)
                {
                    throw new DurableCliException("Could not create migration " + type.FullName + ": " + (e.InnerException ?? e).Message);
                }
            }

            foreach (IGrouping<string, Migration> duplicate in migrations.GroupBy(m => m.Id, StringComparer.Ordinal).Where(g => g.Count() > 1))
                throw new DurableCliException("Migrations " + string.Join(" and ", duplicate.Select(m => m.GetType().FullName)) + " share the Id '" + duplicate.Key + "'.");
            return migrations.OrderBy(m => m.Id, StringComparer.Ordinal).ToList();
        }

        /// <summary>
        /// Returns the entity types: the explicitly named types, or every concrete [Entity] class in the namespace filter.
        /// </summary>
        /// <param name="names">Full or simple type names; empty to discover [Entity] types. Must not be null.</param>
        /// <param name="namespaceFilter">Namespace to restrict discovery to (including nested namespaces), or null for all.</param>
        /// <returns>The entity types, ordered by full name.</returns>
        /// <exception cref="DurableCliException">Thrown when a named type is missing or ambiguous.</exception>
        public List<Type> DiscoverEntities(IReadOnlyList<string> names, string? namespaceFilter)
        {
            ArgumentNullException.ThrowIfNull(names);
            if (names.Count > 0)
            {
                List<Type> selected = new List<Type>();
                foreach (string name in names)
                {
                    List<Type> matches = _Types.Where(t => string.Equals(t.FullName, name, StringComparison.Ordinal)).ToList();
                    if (matches.Count == 0) matches = _Types.Where(t => string.Equals(t.Name, name, StringComparison.Ordinal) && InNamespace(t, namespaceFilter)).ToList();
                    if (matches.Count == 0) throw new DurableCliException("Entity type '" + name + "' was not found in " + System.IO.Path.GetFileName(Path) + ".");
                    if (matches.Count > 1)
                        throw new DurableCliException("Entity type name '" + name + "' is ambiguous (" + string.Join(", ", matches.Select(t => t.FullName)) + "); use the full name.");
                    if (!selected.Contains(matches[0])) selected.Add(matches[0]);
                }

                return selected;
            }

            return _Types
                .Where(t => t.IsClass && !t.IsAbstract && !t.ContainsGenericParameters && t.GetCustomAttribute<EntityAttribute>(false) != null && InNamespace(t, namespaceFilter))
                .OrderBy(t => t.FullName, StringComparer.Ordinal)
                .ToList();
        }

        private List<Type> LoadTypes()
        {
            try
            {
                return Assembly.GetTypes().ToList();
            }
            catch (ReflectionTypeLoadException e)
            {
                string first = e.LoaderExceptions.FirstOrDefault(x => x != null)?.Message ?? "unknown error";
                _Warnings.Add("Some types in " + System.IO.Path.GetFileName(Path) + " could not be loaded and were skipped (" + first + ").");
                return e.Types.Where(t => t != null).Select(t => t!).ToList();
            }
        }

        private void CheckDurableVersions()
        {
            Dictionary<string, Version?> tool = new Dictionary<string, Version?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Durable"] = typeof(EntityAttribute).Assembly.GetName().Version,
                ["Durable.Sql"] = typeof(Migration).Assembly.GetName().Version
            };
            foreach (AssemblyName reference in Assembly.GetReferencedAssemblies())
            {
                if (reference.Name == null || !tool.TryGetValue(reference.Name, out Version? toolVersion) || reference.Version == null || toolVersion == null) continue;
                if (reference.Version.Major != toolVersion.Major || reference.Version.Minor != toolVersion.Minor || reference.Version.Build != toolVersion.Build)
                {
                    _Warnings.Add(System.IO.Path.GetFileName(Path) + " references " + reference.Name + " " + reference.Version.ToString(3) + " but the tool uses " +
                        toolVersion.ToString(3) + "; the tool's version is used. Install a matching Durable.Tool version if commands misbehave.");
                }
            }
        }

        private static bool InNamespace(Type type, string? namespaceFilter)
        {
            if (string.IsNullOrEmpty(namespaceFilter)) return true;
            string ns = type.Namespace ?? string.Empty;
            return string.Equals(ns, namespaceFilter, StringComparison.Ordinal) || ns.StartsWith(namespaceFilter + ".", StringComparison.Ordinal);
        }
    }
}
