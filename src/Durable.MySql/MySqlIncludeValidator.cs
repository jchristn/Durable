namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Validates Include expressions to prevent infinite recursion and ensure valid navigation paths.
    /// Provides protection against circular references and excessive nesting depth.
    /// </summary>
    internal class MySqlIncludeValidator
    {
        #region Public-Members

        /// <summary>
        /// Gets the maximum allowed depth for nested includes.
        /// </summary>
        public int MaxIncludeDepth { get; }

        #endregion

        #region Private-Members

        private readonly HashSet<string> _ValidatedPaths = new HashSet<string>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlIncludeValidator class.
        /// </summary>
        /// <param name="maxIncludeDepth">Maximum depth for nested includes. Default is 5</param>
        /// <exception cref="ArgumentException">Thrown when maxIncludeDepth is less than 1</exception>
        public MySqlIncludeValidator(int maxIncludeDepth = 5)
        {
            if (maxIncludeDepth < 1)
                throw new ArgumentException("Maximum include depth must be at least 1", nameof(maxIncludeDepth));

            MaxIncludeDepth = maxIncludeDepth;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates an include path for proper syntax and depth constraints.
        /// </summary>
        /// <param name="includePath">The include path to validate (e.g., "Company.Address.City")</param>
        /// <exception cref="ArgumentNullException">Thrown when includePath is null</exception>
        /// <exception cref="ArgumentException">Thrown when includePath is empty or contains invalid characters</exception>
        /// <exception cref="InvalidOperationException">Thrown when include depth exceeds maximum allowed depth</exception>
        public void ValidateIncludePath(string includePath)
        {
            if (includePath == null)
                throw new ArgumentNullException(nameof(includePath));

            if (string.IsNullOrWhiteSpace(includePath))
                throw new ArgumentException("Include path cannot be empty or whitespace", nameof(includePath));

            // Check if we've already validated this path
            if (_ValidatedPaths.Contains(includePath))
                return;

            // Split the path and validate each segment
            string[] segments = includePath.Split('.');

            if (segments.Length > MaxIncludeDepth)
            {
                throw new InvalidOperationException($"Include path '{includePath}' exceeds maximum depth of {MaxIncludeDepth}. " +
                    $"Consider reducing nesting or increasing MaxIncludeDepth if necessary.");
            }

            for (int i = 0; i < segments.Length; i++)
            {
                string segment = segments[i];
                ValidatePathSegment(segment, i, includePath);
            }

            // Check for potential circular references
            ValidateCircularReference(segments);

            // Cache the validated path
            _ValidatedPaths.Add(includePath);
        }

        /// <summary>
        /// Validates a collection of include paths for consistency and potential conflicts.
        /// </summary>
        /// <param name="includePaths">The collection of include paths to validate</param>
        /// <exception cref="ArgumentNullException">Thrown when includePaths is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when include paths contain conflicts</exception>
        public void ValidateIncludePaths(IEnumerable<string> includePaths)
        {
            if (includePaths == null)
                throw new ArgumentNullException(nameof(includePaths));

            List<string> pathList = includePaths.ToList();

            // Validate each path individually
            foreach (string path in pathList)
            {
                ValidateIncludePath(path);
            }

            // Check for redundant paths (e.g., including both "Company" and "Company.Address")
            ValidateRedundantPaths(pathList);
        }

        /// <summary>
        /// Clears the internal validation cache. Use this when entity relationships change.
        /// </summary>
        public void ClearValidationCache()
        {
            _ValidatedPaths.Clear();
        }

        #endregion

        #region Private-Methods

        private void ValidatePathSegment(string segment, int segmentIndex, string fullPath)
        {
            if (string.IsNullOrWhiteSpace(segment))
            {
                throw new ArgumentException($"Include path '{fullPath}' contains an empty segment at position {segmentIndex}");
            }

            // Check for valid property name characters
            if (!IsValidPropertyName(segment))
            {
                throw new ArgumentException($"Include path segment '{segment}' contains invalid characters. " +
                    "Property names must start with a letter and contain only letters, digits, and underscores.");
            }

            // Check for reserved keywords that might cause SQL issues
            if (IsReservedKeyword(segment))
            {
                throw new ArgumentException($"Include path segment '{segment}' is a reserved keyword and cannot be used as a property name");
            }
        }

        private void ValidateCircularReference(string[] segments)
        {
            // Simple circular reference detection
            // Check if any segment appears more than once in the path
            Dictionary<string, int> segmentCounts = new Dictionary<string, int>();

            foreach (string segment in segments)
            {
                if (segmentCounts.ContainsKey(segment))
                {
                    segmentCounts[segment]++;
                    if (segmentCounts[segment] > 1)
                    {
                        throw new InvalidOperationException($"Potential circular reference detected: property '{segment}' appears multiple times in the include path");
                    }
                }
                else
                {
                    segmentCounts[segment] = 1;
                }
            }
        }

        private void ValidateRedundantPaths(List<string> includePaths)
        {
            // Sort paths by length (shorter first)
            List<string> sortedPaths = includePaths.OrderBy(p => p.Length).ToList();

            for (int i = 0; i < sortedPaths.Count; i++)
            {
                string currentPath = sortedPaths[i];

                for (int j = i + 1; j < sortedPaths.Count; j++)
                {
                    string longerPath = sortedPaths[j];

                    // Check if the longer path starts with the current path + "."
                    if (longerPath.StartsWith(currentPath + "."))
                    {
                        throw new InvalidOperationException($"Redundant include paths detected: '{currentPath}' is already included by '{longerPath}'. " +
                            "Remove the shorter path as it's automatically included by the longer one.");
                    }
                }
            }
        }

        private bool IsValidPropertyName(string name)
        {
            if (string.IsNullOrEmpty(name))
                return false;

            // Must start with a letter
            if (!char.IsLetter(name[0]))
                return false;

            // Rest can be letters, digits, or underscores
            for (int i = 1; i < name.Length; i++)
            {
                char c = name[i];
                if (!char.IsLetterOrDigit(c) && c != '_')
                    return false;
            }

            return true;
        }

        private bool IsReservedKeyword(string name)
        {
            // Common SQL and C# reserved keywords that might cause issues
            string[] reservedKeywords = {
                "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
                "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE",
                "INDEX", "PRIMARY", "KEY", "FOREIGN", "CONSTRAINT", "NULL", "NOT",
                "AND", "OR", "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET",
                "CLASS", "NAMESPACE", "USING", "PUBLIC", "PRIVATE", "PROTECTED",
                "INTERNAL", "STATIC", "VIRTUAL", "OVERRIDE", "ABSTRACT", "SEALED"
            };

            return reservedKeywords.Contains(name.ToUpperInvariant());
        }

        #endregion
    }
}