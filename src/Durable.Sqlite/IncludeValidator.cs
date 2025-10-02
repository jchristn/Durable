namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Validates Include expressions to prevent infinite recursion and ensure valid navigation paths.
    /// Provides protection against circular references and excessive nesting depth.
    /// </summary>
    internal class IncludeValidator
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly CycleDetector _CycleDetector;
        private readonly HashSet<string> _ProcessedIncludes;
        private readonly int _MaxIncludeDepth;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the IncludeValidator class.
        /// </summary>
        /// <param name="maxIncludeDepth">Maximum depth for nested includes. Default is 5</param>
        /// <exception cref="ArgumentException">Thrown when maxIncludeDepth is less than 1</exception>
        public IncludeValidator(int maxIncludeDepth = 5)
        {
            _CycleDetector = new CycleDetector();
            _ProcessedIncludes = new HashSet<string>();
            _MaxIncludeDepth = maxIncludeDepth;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates an include path for circular references, depth constraints, and duplicates.
        /// </summary>
        /// <param name="includePath">The include path to validate (e.g., "Company.Address")</param>
        /// <param name="sourceType">The source entity type</param>
        /// <param name="targetType">The target entity type</param>
        /// <exception cref="ArgumentNullException">Thrown when any parameter is null</exception>
        /// <exception cref="InvalidOperationException">Thrown when validation fails (circular reference, duplicate path, or excessive depth)</exception>
        public void ValidateInclude(string includePath, Type sourceType, Type targetType)
        {
            // Check for duplicate includes
            if (_ProcessedIncludes.Contains(includePath))
            {
                throw new InvalidOperationException($"Include path '{includePath}' has already been added");
            }

            // Check depth
            int depth = includePath.Split('.').Length;
            if (depth > _MaxIncludeDepth)
            {
                throw new InvalidOperationException($"Include depth exceeds maximum allowed depth of {_MaxIncludeDepth}");
            }

            // Check for cycles
            if (_CycleDetector.WouldCreateCycle(includePath, sourceType, targetType))
            {
                throw new InvalidOperationException($"Include path '{includePath}' would create a circular reference");
            }

            _ProcessedIncludes.Add(includePath);
        }

        /// <summary>
        /// Clears all tracked validation state and resets the validator to initial state.
        /// </summary>
        public void Reset()
        {
            _CycleDetector.Reset();
            _ProcessedIncludes.Clear();
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}