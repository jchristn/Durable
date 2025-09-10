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

        private readonly HashSet<string> _VisitedPaths;
        private readonly Dictionary<Type, HashSet<Type>> _TypeGraph;

        #endregion

        #region Constructors-and-Factories

        public CycleDetector()
        {
            _VisitedPaths = new HashSet<string>();
            _TypeGraph = new Dictionary<Type, HashSet<Type>>();
        }

        #endregion

        #region Public-Methods

        public bool WouldCreateCycle(string includePath, Type sourceType, Type targetType)
        {
            // Check if this path has already been visited
            if (_VisitedPaths.Contains(includePath))
            {
                return false; // Already processed, no cycle
            }

            // Check for type-level cycles
            if (!_TypeGraph.ContainsKey(sourceType))
            {
                _TypeGraph[sourceType] = new HashSet<Type>();
            }

            if (_TypeGraph[sourceType].Contains(targetType))
            {
                // Check if there's a reverse path
                if (_TypeGraph.ContainsKey(targetType) && _TypeGraph[targetType].Contains(sourceType))
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
            _TypeGraph[sourceType].Add(targetType);
            _VisitedPaths.Add(includePath);
            return false;
        }

        public void Reset()
        {
            _VisitedPaths.Clear();
            _TypeGraph.Clear();
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

            if (_TypeGraph.ContainsKey(current))
            {
                foreach (Type neighbor in _TypeGraph[current])
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
}