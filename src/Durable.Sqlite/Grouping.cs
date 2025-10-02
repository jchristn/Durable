namespace Durable.Sqlite
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents a grouping of elements with a common key for SQLite grouped query operations.
    /// Provides enhanced navigation property support and complex type handling.
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key</typeparam>
    /// <typeparam name="TElement">The type of the elements in the group</typeparam>
    internal class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private readonly TKey _Key;
        private readonly List<TElement> _Elements;

        /// <summary>
        /// Initializes a new instance of the Grouping class.
        /// </summary>
        /// <param name="key">The common key for all elements in this group</param>
        /// <param name="elements">The elements that share this key</param>
        /// <exception cref="ArgumentNullException">Thrown when elements is null</exception>
        public Grouping(TKey key, IEnumerable<TElement> elements)
        {
            _Key = key;
            _Elements = new List<TElement>(elements);
        }

        /// <summary>
        /// Gets the key that is common to all elements in this group.
        /// </summary>
        public TKey Key => _Key;

        /// <summary>
        /// Returns an enumerator that iterates through the elements in this group.
        /// </summary>
        /// <returns>An enumerator for the elements in this group</returns>
        public IEnumerator<TElement> GetEnumerator()
        {
            return _Elements.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through the elements in this group.
        /// </summary>
        /// <returns>An enumerator for the elements in this group</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}