namespace Durable.CosmosDb
{
    using System;

    /// <summary>
    /// A Cosmos DB SQL condition translated from a <see cref="Durable.Query.QueryNode"/>. The text always evaluates to true
    /// or false for every document (never undefined), so it can be negated and combined freely. It is a necessary condition
    /// of the node: documents it rejects never satisfy the node; when <see cref="Exact"/> is true it is also sufficient.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    internal sealed class CosmosDbSqlFragment
    {
        /// <summary>
        /// Gets the condition text. Never null.
        /// </summary>
        public string Text { get; }

        /// <summary>
        /// Gets whether the condition is exactly the node (no client-side check needed).
        /// </summary>
        public bool Exact { get; }

        /// <summary>
        /// Instantiates a fragment.
        /// </summary>
        /// <param name="text">Condition text. Must not be null.</param>
        /// <param name="exact">Whether the condition is exactly the node.</param>
        /// <exception cref="ArgumentNullException">Thrown when text is null.</exception>
        public CosmosDbSqlFragment(string text, bool exact)
        {
            Text = text ?? throw new ArgumentNullException(nameof(text));
            Exact = exact;
        }
    }
}
