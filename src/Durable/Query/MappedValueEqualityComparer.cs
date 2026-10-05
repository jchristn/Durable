namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Compares objects by the values of their mapped columns (the shape a database row has), so client-side
    /// <c>Distinct</c> over projected results removes rows with equal values rather than equal references.
    /// Types without mapped columns fall back to <see cref="object.Equals(object)"/>.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    /// <typeparam name="T">Compared type.</typeparam>
    internal sealed class MappedValueEqualityComparer<T> : IEqualityComparer<T>
    {
        #region Private-Members

        private readonly IReadOnlyList<ColumnMetadata> _Columns;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a comparer for <typeparamref name="T"/>.
        /// </summary>
        public MappedValueEqualityComparer()
        {
            _Columns = EntityMetadata.For(typeof(T)).Columns;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public bool Equals(T? x, T? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x == null || y == null) return false;
            if (_Columns.Count == 0) return x.Equals(y);
            foreach (ColumnMetadata column in _Columns)
            {
                if (!QueryValueComparer.Ordinal.Equals(column.GetValue(x), column.GetValue(y))) return false;
            }

            return true;
        }

        /// <inheritdoc />
        public int GetHashCode(T obj)
        {
            if (obj == null) return 0;
            if (_Columns.Count == 0) return obj.GetHashCode();
            HashCode hash = new HashCode();
            foreach (ColumnMetadata column in _Columns) hash.Add(QueryValueComparer.Ordinal.GetHashCode(column.GetValue(obj)));
            return hash.ToHashCode();
        }

        #endregion
    }
}
