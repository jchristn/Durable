namespace Durable.Postgres
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents a grouping of elements with a common key for PostgreSQL grouped query operations.
    /// Provides enhanced navigation property support and complex type handling.
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key</typeparam>
    /// <typeparam name="TElement">The type of the elements in the group</typeparam>
    internal class PostgresGrouping<TKey, TElement> : IGrouping<TKey, TElement>
    {

        #region Public-Members

        #endregion

        #region Private-Members

        private readonly TKey _Key;
        private readonly List<TElement> _Elements;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresGrouping class.
        /// </summary>
        /// <param name="key">The common key for all elements in this group</param>
        /// <param name="elements">The elements that share this key</param>
        /// <exception cref="ArgumentNullException">Thrown when elements is null</exception>
        public PostgresGrouping(TKey key, IEnumerable<TElement> elements)
        {
            if (elements == null)
                throw new ArgumentNullException(nameof(elements));

            _Key = key;
            _Elements = new List<TElement>(elements);
        }

        #endregion

        #region Public-Methods

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

        #endregion

        #region Private-Methods

        #endregion
    }
}