namespace Durable.Sqlite
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;

    internal class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private readonly TKey _Key;
        private readonly List<TElement> _Elements;

        public Grouping(TKey key, IEnumerable<TElement> elements)
        {
            _Key = key;
            _Elements = new List<TElement>(elements);
        }

        public TKey Key => _Key;

        public IEnumerator<TElement> GetEnumerator()
        {
            return _Elements.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}