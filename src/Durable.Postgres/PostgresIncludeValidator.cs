namespace Durable.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Validates Include expressions to prevent common errors and security issues in PostgreSQL queries.
    /// Provides depth limiting, circular reference detection, and path validation.
    /// </summary>
    internal class PostgresIncludeValidator
    {

        #region Public-Members

        #endregion

        #region Private-Members

        private readonly int _MaxIncludeDepth;
        private readonly HashSet<string> _ValidatedPaths = new HashSet<string>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresIncludeValidator class.
        /// </summary>
        /// <param name="maxIncludeDepth">Maximum allowed depth for nested includes. Default is 5</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when maxIncludeDepth is less than 1</exception>
        public PostgresIncludeValidator(int maxIncludeDepth = 5)
        {
            if (maxIncludeDepth < 1)
                throw new ArgumentOutOfRangeException(nameof(maxIncludeDepth), "Maximum include depth must be at least 1");

            _MaxIncludeDepth = maxIncludeDepth;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates an include path to ensure it meets safety and complexity requirements.
        /// </summary>
        /// <param name="includePath">The include path to validate (e.g., "Company.Address.Country")</param>
        /// <exception cref="ArgumentNullException">Thrown when includePath is null</exception>
        /// <exception cref="ArgumentException">Thrown when includePath is invalid</exception>
        /// <exception cref="InvalidOperationException">Thrown when include depth exceeds the maximum allowed</exception>
        public void ValidateIncludePath(string includePath)
        {
            if (includePath == null)
                throw new ArgumentNullException(nameof(includePath));

            if (string.IsNullOrWhiteSpace(includePath))
                throw new ArgumentException("Include path cannot be empty or whitespace", nameof(includePath));

            // Check if already validated
            if (_ValidatedPaths.Contains(includePath))
                return;

            ValidatePathSyntax(includePath);
            ValidatePathDepth(includePath);
            ValidatePathSecurity(includePath);

            _ValidatedPaths.Add(includePath);
        }

        /// <summary>
        /// Validates multiple include paths at once for batch processing.
        /// </summary>
        /// <param name="includePaths">The collection of include paths to validate</param>
        /// <exception cref="ArgumentNullException">Thrown when includePaths is null</exception>
        public void ValidateIncludePaths(IEnumerable<string> includePaths)
        {
            if (includePaths == null)
                throw new ArgumentNullException(nameof(includePaths));

            foreach (string path in includePaths)
            {
                ValidateIncludePath(path);
            }
        }

        /// <summary>
        /// Clears the validation cache, forcing re-validation of all paths.
        /// This method is useful for testing or when validation rules change.
        /// </summary>
        public void ClearValidationCache()
        {
            _ValidatedPaths.Clear();
        }

        #endregion

        #region Private-Methods

        private void ValidatePathSyntax(string includePath)
        {
            // Check for invalid characters
            if (includePath.Contains("..") || includePath.Contains("//"))
                throw new ArgumentException($"Include path contains invalid sequences: {includePath}", nameof(includePath));

            // Check for SQL injection patterns
            string lowerPath = includePath.ToLowerInvariant();
            string[] dangerousPatterns = {
                "select", "insert", "update", "delete", "drop", "create", "alter",
                "exec", "execute", "sp_", "xp_", "--", "/*", "*/", "union", "script"
            };

            foreach (string pattern in dangerousPatterns)
            {
                if (lowerPath.Contains(pattern))
                    throw new ArgumentException($"Include path contains potentially dangerous content: {includePath}", nameof(includePath));
            }

            // Validate path segments
            string[] segments = includePath.Split('.');
            foreach (string segment in segments)
            {
                ValidatePathSegment(segment);
            }
        }

        private void ValidatePathSegment(string segment)
        {
            if (string.IsNullOrWhiteSpace(segment))
                throw new ArgumentException("Include path segments cannot be empty");

            // Check length
            if (segment.Length > 64)
                throw new ArgumentException($"Include path segment is too long (max 64 characters): {segment}");

            // Check for valid identifier characters
            if (!char.IsLetter(segment[0]) && segment[0] != '_')
                throw new ArgumentException($"Include path segment must start with a letter or underscore: {segment}");

            for (int i = 1; i < segment.Length; i++)
            {
                char c = segment[i];
                if (!char.IsLetterOrDigit(c) && c != '_')
                    throw new ArgumentException($"Include path segment contains invalid character '{c}': {segment}");
            }
        }

        private void ValidatePathDepth(string includePath)
        {
            int depth = includePath.Split('.').Length;
            if (depth > _MaxIncludeDepth)
                throw new InvalidOperationException($"Include path depth ({depth}) exceeds maximum allowed depth ({_MaxIncludeDepth}): {includePath}");
        }

        private void ValidatePathSecurity(string includePath)
        {
            // Additional security checks can be added here
            // For example, checking against a whitelist of allowed navigation properties
            // or validating against known entity relationships

            // Check for excessively long paths that might indicate an attack
            if (includePath.Length > 500)
                throw new ArgumentException($"Include path is too long (max 500 characters): {includePath}");

            // Check for suspicious repetitive patterns
            if (HasSuspiciousPatterns(includePath))
                throw new ArgumentException($"Include path contains suspicious patterns: {includePath}");
        }

        private bool HasSuspiciousPatterns(string includePath)
        {
            // Check for repetitive segments that might indicate an attempt to cause issues
            string[] segments = includePath.Split('.');
            if (segments.Length > 3)
            {
                // Look for repeated segments
                Dictionary<string, int> segmentCounts = new Dictionary<string, int>();
                foreach (string segment in segments)
                {
                    segmentCounts[segment] = segmentCounts.GetValueOrDefault(segment, 0) + 1;
                    if (segmentCounts[segment] > 2)
                        return true; // Same segment repeated more than twice
                }
            }

            return false;
        }

        #endregion
    }
}