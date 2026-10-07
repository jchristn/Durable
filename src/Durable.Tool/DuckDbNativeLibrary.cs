namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;

    /// <summary>
    /// Locates the DuckDB native library (libduckdb) at run time. The tool package does not bundle it (it is about 330 MB per
    /// target framework across platforms); a project that references Durable.DuckDb has it in its build output, so the
    /// library is resolved, in order, from: the user's assembly (its <c>.deps.json</c>, its directory and its
    /// <c>runtimes/&lt;rid&gt;/native/</c> directories), the <see cref="EnvironmentVariable"/> path, then the default
    /// probing of the runtime. Thread-safe.
    /// </summary>
    internal static class DuckDbNativeLibrary
    {
        /// <summary>
        /// Gets the environment variable holding the path of the DuckDB native library (or of the directory that contains it),
        /// used when the user's build output has none.
        /// </summary>
        public const string EnvironmentVariable = "DURABLE_DUCKDB_NATIVE";

        /// <summary>
        /// Gets the name of the managed assembly whose P/Invokes load libduckdb.
        /// </summary>
        public const string BindingsAssemblyName = "DuckDB.NET.Bindings";

        /// <summary>
        /// Gets the message reported when libduckdb cannot be found, explaining how to fix it.
        /// </summary>
        public static string MissingMessage =>
            "the DuckDB native library (" + LibraryFileName + ") was not found. The durable tool does not bundle it: reference " +
            "Durable.DuckDb in the project the tool builds (--project) or loads (--assembly) and build it, so the library is in " +
            "its output, or set " + EnvironmentVariable + " to the path of " + LibraryFileName + " (for example from the " +
            "DuckDB.NET.Bindings.Full package or a DuckDB release).";

        /// <summary>
        /// Gets the platform file name of the library (duckdb.dll, libduckdb.dylib or libduckdb.so).
        /// </summary>
        public static string LibraryFileName
        {
            get
            {
                if (OperatingSystem.IsWindows()) return "duckdb.dll";
                if (OperatingSystem.IsMacOS()) return "libduckdb.dylib";
                return "libduckdb.so";
            }
        }

        private static readonly object _Lock = new object();
        private static readonly List<string> _AssemblyPaths = new List<string>();
        private static bool _Registered = false;
        private static IntPtr _Handle = IntPtr.Zero;
        private static string? _OverridePath = null;

        /// <summary>
        /// Adds a user assembly whose build output is probed for the library. Later additions are probed first.
        /// </summary>
        /// <param name="assemblyPath">Absolute path of the user's assembly. Must not be null.</param>
        public static void AddAssembly(string assemblyPath)
        {
            ArgumentNullException.ThrowIfNull(assemblyPath);
            lock (_Lock)
            {
                _AssemblyPaths.Remove(assemblyPath);
                _AssemblyPaths.Insert(0, assemblyPath);
            }
        }

        /// <summary>
        /// Registers the resolver for the DuckDB bindings' native imports. Idempotent; does nothing when another component
        /// already registered a resolver for the bindings (the runtime then probes as usual).
        /// </summary>
        public static void Register()
        {
            lock (_Lock)
            {
                if (_Registered) return;
                _Registered = true;
                Assembly? bindings = GetBindingsAssembly();
                if (bindings == null) return;
                try
                {
                    NativeLibrary.SetDllImportResolver(bindings, Resolve);
                }
                catch (InvalidOperationException)
                {
                    // A resolver is already set for the bindings (for example by a host process); keep it.
                }
            }
        }

        /// <summary>
        /// Registers the resolver (<see cref="Register"/>) and loads the library, so a missing library is reported before any
        /// DuckDB type is touched (DuckDB.NET's type initializers call into it, and a failed type initializer stays failed for
        /// the life of the process). Once loaded, the same library is used for the rest of the process.
        /// </summary>
        /// <param name="overridePath">The <see cref="EnvironmentVariable"/> value (a library file or its directory), already
        /// resolved against the working directory; null or empty for none.</param>
        /// <exception cref="DurableCliException">Thrown when the library cannot be found; the message says how to fix it.</exception>
        public static void EnsureLoaded(string? overridePath)
        {
            lock (_Lock)
            {
                _OverridePath = GetOverridePath(overridePath);
            }

            Register();
            Assembly? bindings = GetBindingsAssembly();
            if (Resolve("duckdb", bindings ?? typeof(DuckDbNativeLibrary).Assembly, null) == IntPtr.Zero)
                throw new DurableCliException(MissingMessage);
        }

        /// <summary>
        /// Returns the candidate paths of the library for an assembly, most specific first: the <c>.deps.json</c> resolution,
        /// the assembly directory, then <c>runtimes/&lt;rid&gt;/native/</c> for the current and fallback runtime identifiers.
        /// </summary>
        /// <param name="assemblyPath">Absolute path of the user's assembly. Must not be null.</param>
        /// <returns>Candidate file paths; they may not exist. Never null.</returns>
        public static List<string> GetCandidates(string assemblyPath)
        {
            ArgumentNullException.ThrowIfNull(assemblyPath);
            List<string> candidates = new List<string>();
            try
            {
                string? resolved = new AssemblyDependencyResolver(assemblyPath).ResolveUnmanagedDllToPath("duckdb");
                if (resolved != null) candidates.Add(resolved);
            }
            catch (InvalidOperationException)
            {
                // No usable .deps.json; fall back to the directory layout below.
            }

            string directory = Path.GetDirectoryName(assemblyPath) ?? ".";
            candidates.Add(Path.Combine(directory, LibraryFileName));
            foreach (string rid in GetRuntimeIdentifiers()) candidates.Add(Path.Combine(directory, "runtimes", rid, "native", LibraryFileName));
            return candidates.Distinct(StringComparer.Ordinal).ToList();
        }

        /// <summary>
        /// Returns the path named by <see cref="EnvironmentVariable"/>: the value itself when it is a file, or the library file
        /// inside it when it is a directory.
        /// </summary>
        /// <param name="value">The variable's value; may be null or empty.</param>
        /// <returns>The path, or null when the value is empty.</returns>
        public static string? GetOverridePath(string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return null;
            string trimmed = value.Trim();
            return Directory.Exists(trimmed) ? Path.Combine(trimmed, LibraryFileName) : trimmed;
        }

        /// <summary>
        /// Returns whether an exception (or one of its inner exceptions) reports that libduckdb could not be loaded.
        /// </summary>
        /// <param name="exception">Exception. Must not be null.</param>
        /// <returns>True when DuckDB's native library is missing.</returns>
        public static bool IsMissingLibrary(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            for (Exception? current = exception; current != null; current = current.InnerException)
            {
                if (current is DllNotFoundException && current.Message.IndexOf("duckdb", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            }

            return false;
        }

        private static IntPtr Resolve(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
        {
            if (!IsDuckDbName(libraryName)) return IntPtr.Zero;
            lock (_Lock)
            {
                if (_Handle != IntPtr.Zero) return _Handle;
                List<string> candidates = new List<string>();
                foreach (string assemblyPath in _AssemblyPaths) candidates.AddRange(GetCandidates(assemblyPath));
                if (_OverridePath != null) candidates.Add(_OverridePath);
                foreach (string candidate in candidates)
                {
                    if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out IntPtr handle))
                    {
                        _Handle = handle;
                        return handle;
                    }
                }

                if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out IntPtr fallback))
                {
                    _Handle = fallback;
                    return fallback;
                }

                return IntPtr.Zero;
            }
        }

        private static Assembly? GetBindingsAssembly()
        {
            try
            {
                return AssemblyLoadContext.Default.LoadFromAssemblyName(new AssemblyName(BindingsAssemblyName));
            }
            catch (IOException)
            {
                return null;
            }
        }

        private static bool IsDuckDbName(string libraryName)
        {
            string name = Path.GetFileNameWithoutExtension(libraryName);
            if (name.StartsWith("lib", StringComparison.Ordinal)) name = name.Substring(3);
            return string.Equals(name, "duckdb", StringComparison.OrdinalIgnoreCase);
        }

        private static List<string> GetRuntimeIdentifiers()
        {
            string os = OperatingSystem.IsWindows() ? "win" : OperatingSystem.IsMacOS() ? "osx" : "linux";
            string arch = RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant();
            List<string> rids = new List<string> { RuntimeInformation.RuntimeIdentifier, os + "-" + arch };
            if (os == "linux") rids.Add("linux-musl-" + arch);
            rids.Add(os);
            if (os == "osx") rids.Add("unix");
            return rids.Distinct(StringComparer.Ordinal).ToList();
        }
    }
}
