namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;

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

        public IncludeValidator(int maxIncludeDepth = 5)
        {
            _CycleDetector = new CycleDetector();
            _ProcessedIncludes = new HashSet<string>();
            _MaxIncludeDepth = maxIncludeDepth;
        }

        #endregion

        #region Public-Methods

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