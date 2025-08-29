namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal class CycleDetector
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly HashSet<string> _visitedPaths;
        private readonly Dictionary<Type, HashSet<Type>> _typeGraph;

        #endregion

        #region Constructors-and-Factories

        public CycleDetector()
        {
            _visitedPaths = new HashSet<string>();
            _typeGraph = new Dictionary<Type, HashSet<Type>>();
        }

        #endregion

        #region Public-Methods

        public bool WouldCreateCycle(string includePath, Type sourceType, Type targetType)
        {
            // Check if this path has already been visited
            if (_visitedPaths.Contains(includePath))
            {
                return false; // Already processed, no cycle
            }

            // Check for type-level cycles
            if (!_typeGraph.ContainsKey(sourceType))
            {
                _typeGraph[sourceType] = new HashSet<Type>();
            }

            if (_typeGraph[sourceType].Contains(targetType))
            {
                // Check if there's a reverse path
                if (_typeGraph.ContainsKey(targetType) && _typeGraph[targetType].Contains(sourceType))
                {
                    return true; // Cycle detected
                }
            }

            // Check for deeper cycles using DFS
            HashSet<Type> visited = new HashSet<Type>();
            if (HasCycleDFS(sourceType, targetType, visited))
            {
                return true;
            }

            // No cycle detected, add to graph
            _typeGraph[sourceType].Add(targetType);
            _visitedPaths.Add(includePath);
            return false;
        }

        public void Reset()
        {
            _visitedPaths.Clear();
            _typeGraph.Clear();
        }

        #endregion

        #region Private-Methods

        private bool HasCycleDFS(Type current, Type target, HashSet<Type> visited)
        {
            if (visited.Contains(current))
            {
                return current == target; // Cycle found if we're back at the target
            }

            visited.Add(current);

            if (_typeGraph.ContainsKey(current))
            {
                foreach (Type neighbor in _typeGraph[current])
                {
                    if (HasCycleDFS(neighbor, target, visited))
                    {
                        return true;
                    }
                }
            }

            visited.Remove(current);
            return false;
        }

        #endregion
    }

    internal class IncludeValidator
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly CycleDetector _cycleDetector;
        private readonly HashSet<string> _processedIncludes;
        private readonly int _maxIncludeDepth;

        #endregion

        #region Constructors-and-Factories

        public IncludeValidator(int maxIncludeDepth = 5)
        {
            _cycleDetector = new CycleDetector();
            _processedIncludes = new HashSet<string>();
            _maxIncludeDepth = maxIncludeDepth;
        }

        #endregion

        #region Public-Methods

        public void ValidateInclude(string includePath, Type sourceType, Type targetType)
        {
            // Check for duplicate includes
            if (_processedIncludes.Contains(includePath))
            {
                throw new InvalidOperationException($"Include path '{includePath}' has already been added");
            }

            // Check depth
            int depth = includePath.Split('.').Length;
            if (depth > _maxIncludeDepth)
            {
                throw new InvalidOperationException($"Include depth exceeds maximum allowed depth of {_maxIncludeDepth}");
            }

            // Check for cycles
            if (_cycleDetector.WouldCreateCycle(includePath, sourceType, targetType))
            {
                throw new InvalidOperationException($"Include path '{includePath}' would create a circular reference");
            }

            _processedIncludes.Add(includePath);
        }

        public void Reset()
        {
            _cycleDetector.Reset();
            _processedIncludes.Clear();
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}