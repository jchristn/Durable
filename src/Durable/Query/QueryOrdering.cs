namespace Durable.Query
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// One ordering key of a <see cref="QueryModel"/>.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class QueryOrdering
    {
        #region Public-Members

        /// <summary>
        /// Gets the key, normalized against the model's source. Never null.
        /// </summary>
        public QueryNode Key { get; }

        /// <summary>
        /// Gets the original key selector, for backends that evaluate on the client. Never null.
        /// </summary>
        public LambdaExpression KeySelector { get; }

        /// <summary>
        /// Gets whether the order is descending.
        /// </summary>
        public bool Descending { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates an ordering.
        /// </summary>
        /// <param name="key">Normalized key. Must not be null.</param>
        /// <param name="keySelector">Original key selector. Must not be null.</param>
        /// <param name="descending">Whether the order is descending.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public QueryOrdering(QueryNode key, LambdaExpression keySelector, bool descending)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            KeySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            Descending = descending;
        }

        #endregion
    }
}
