namespace Durable.InMemory
{
    using System;
    using System.Globalization;
    using System.Linq;
    using Durable.Query;

    /// <summary>
    /// The primary key of a stored row: one normalized value per key column (integers of any width compare equal through
    /// <see cref="KeyNormalizer"/>; strings compare ordinally, so keys are case-sensitive).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class InMemoryRowKey : IEquatable<InMemoryRowKey>
    {
        #region Public-Members

        /// <summary>
        /// Gets the normalized key parts in key order. Never null.
        /// </summary>
        public object?[] Parts { get; }

        #endregion

        #region Private-Members

        private readonly int _Hash;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a key from stored key values.
        /// </summary>
        /// <param name="values">Stored key values in key order. Must not be null.</param>
        public InMemoryRowKey(object?[] values)
        {
            Parts = values.Select(KeyNormalizer.Normalize).ToArray();
            HashCode hash = new HashCode();
            foreach (object? part in Parts) hash.Add(QueryValueComparer.Ordinal.GetHashCode(part));
            _Hash = hash.ToHashCode();
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public bool Equals(InMemoryRowKey? other)
        {
            if (other == null || other.Parts.Length != Parts.Length) return false;
            for (int i = 0; i < Parts.Length; i++)
            {
                if (!QueryValueComparer.Ordinal.Equals(Parts[i], other.Parts[i])) return false;
            }

            return true;
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            return obj is InMemoryRowKey other && Equals(other);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return _Hash;
        }

        /// <inheritdoc />
        public override string ToString()
        {
            if (Parts.Length == 1) return Convert.ToString(Parts[0], CultureInfo.InvariantCulture) ?? "null";
            return "(" + string.Join(", ", Parts.Select(p => Convert.ToString(p, CultureInfo.InvariantCulture) ?? "null")) + ")";
        }

        #endregion
    }
}
