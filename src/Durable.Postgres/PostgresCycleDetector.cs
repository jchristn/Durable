namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Detects cycles in navigation property includes to prevent infinite loops in PostgreSQL queries.
    /// Ensures that Include() chains don't create circular references that would cause stack overflow.
    /// </summary>
    internal class PostgresCycleDetector
    {

        #region Private-Members

        private readonly HashSet<string> _VisitedPaths;
        private readonly Stack<string> _CurrentPath;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresCycleDetector.
        /// </summary>
        public PostgresCycleDetector()
        {
            _VisitedPaths = new HashSet<string>();
            _CurrentPath = new Stack<string>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Checks if adding the specified path would create a cycle.
        /// </summary>
        /// <param name="path">The navigation property path to check</param>
        /// <returns>True if adding this path would create a cycle, false otherwise</returns>
        /// <exception cref="ArgumentNullException">Thrown when path is null</exception>
        /// <exception cref="ArgumentException">Thrown when path is empty or whitespace</exception>
        public bool WouldCreateCycle(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be null or empty", nameof(path));

            // Normalize the path
            string normalizedPath = NormalizePath(path);

            // Check if we're already processing this path (immediate cycle)
            if (_CurrentPath.Contains(normalizedPath))
                return true;

            // Check if this path would create a longer cycle
            string fullPath = BuildFullPath(normalizedPath);
            return _VisitedPaths.Any(visitedPath =>
                visitedPath.Contains(fullPath) || fullPath.Contains(visitedPath));
        }

        /// <summary>
        /// Adds a path to the current processing stack and visited paths.
        /// </summary>
        /// <param name="path">The navigation property path to add</param>
        /// <exception cref="ArgumentNullException">Thrown when path is null</exception>
        /// <exception cref="ArgumentException">Thrown when path is empty or whitespace</exception>
        /// <exception cref="InvalidOperationException">Thrown when adding this path would create a cycle</exception>
        public void EnterPath(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be null or empty", nameof(path));

            string normalizedPath = NormalizePath(path);

            if (WouldCreateCycle(normalizedPath))
                throw new InvalidOperationException($"Adding path '{path}' would create a cycle in navigation properties");

            _CurrentPath.Push(normalizedPath);
            string fullPath = BuildFullPath(normalizedPath);
            _VisitedPaths.Add(fullPath);
        }

        /// <summary>
        /// Removes the most recently added path from the current processing stack.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when there are no paths to exit</exception>
        public void ExitPath()
        {
            if (_CurrentPath.Count == 0)
                throw new InvalidOperationException("No path to exit");

            _CurrentPath.Pop();
        }

        /// <summary>
        /// Gets the current navigation path as a dot-separated string.
        /// </summary>
        /// <returns>The current path or empty string if no path is active</returns>
        public string GetCurrentPath()
        {
            if (_CurrentPath.Count == 0)
                return string.Empty;

            return string.Join(".", _CurrentPath.Reverse());
        }

        /// <summary>
        /// Clears all tracked paths and resets the detector to initial state.
        /// </summary>
        public void Reset()
        {
            _VisitedPaths.Clear();
            _CurrentPath.Clear();
        }

        /// <summary>
        /// Gets the depth of the current navigation path.
        /// </summary>
        /// <returns>The number of navigation levels deep</returns>
        public int GetCurrentDepth()
        {
            return _CurrentPath.Count;
        }

        /// <summary>
        /// Checks if the detector has any active paths.
        /// </summary>
        /// <returns>True if there are active paths, false otherwise</returns>
        public bool HasActivePaths()
        {
            return _CurrentPath.Count > 0;
        }

        /// <summary>
        /// Gets a copy of all visited paths for debugging purposes.
        /// </summary>
        /// <returns>A collection of all visited navigation paths</returns>
        public IEnumerable<string> GetVisitedPaths()
        {
            return _VisitedPaths.ToList();
        }

        #endregion

        #region Private-Methods

        private static string NormalizePath(string path)
        {
            return path.Trim().Replace(" ", "");
        }

        private string BuildFullPath(string newPath)
        {
            if (_CurrentPath.Count == 0)
                return newPath;

            return string.Join(".", _CurrentPath.Reverse()) + "." + newPath;
        }

        #endregion
    }
}