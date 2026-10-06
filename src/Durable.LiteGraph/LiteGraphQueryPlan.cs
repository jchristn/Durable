namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Describes what a <see cref="LiteGraphBackend"/> asked LiteGraph for when it read candidate nodes: raised through
    /// <see cref="LiteGraphBackend.QueryPlanned"/> and logged at debug level. Push-down only narrows the candidates; the
    /// complete Durable filter is always evaluated client-side afterwards with C# semantics, so push-down never changes
    /// results.
    /// Thread safety: immutable.
    /// </summary>
    public sealed class LiteGraphQueryPlan : EventArgs
    {
        #region Public-Members

        /// <summary>
        /// Gets the backend operation that read the nodes: <c>Query</c>, <c>Count</c>, <c>Aggregate</c>, <c>Replace</c>,
        /// <c>Update</c>, <c>Delete</c>, <c>Related</c> (rows of a related entity for navigations) or <c>Dependents</c>
        /// (rows referencing a new principal, for edge maintenance). Never null.
        /// </summary>
        public string Operation { get; }

        /// <summary>
        /// Gets the entity type read. Never null.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Gets the node label read (the entity's table name). Never null.
        /// </summary>
        public string Label { get; }

        /// <summary>
        /// Gets how the nodes were read.
        /// </summary>
        public LiteGraphReadStrategy Strategy { get; }

        /// <summary>
        /// Gets the node GUIDs read for <see cref="LiteGraphReadStrategy.KeyLookup"/>; empty otherwise. Never null.
        /// </summary>
        public IReadOnlyList<Guid> NodeGuids { get; }

        /// <summary>
        /// Gets the LiteGraph data filter pushed down with a <see cref="LiteGraphReadStrategy.LabelScan"/>, in
        /// ExpressionTree text form; null when nothing was pushed down.
        /// </summary>
        public string? DataFilter { get; }

        /// <summary>
        /// Gets whether the read was made inside a transaction (whose own writes are merged into the results).
        /// </summary>
        public bool InTransaction { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a plan description.
        /// </summary>
        /// <param name="operation">Operation. Must not be null.</param>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="label">Node label. Must not be null.</param>
        /// <param name="strategy">Strategy.</param>
        /// <param name="nodeGuids">Node GUIDs for a key lookup; null for none.</param>
        /// <param name="dataFilter">Pushed-down data filter text; null for none.</param>
        /// <param name="inTransaction">Whether the read was made inside a transaction.</param>
        /// <exception cref="ArgumentNullException">Thrown when operation, entityType or label is null.</exception>
        public LiteGraphQueryPlan(string operation, Type entityType, string label, LiteGraphReadStrategy strategy, IReadOnlyList<Guid>? nodeGuids, string? dataFilter, bool inTransaction)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            Label = label ?? throw new ArgumentNullException(nameof(label));
            Strategy = strategy;
            NodeGuids = nodeGuids ?? Array.Empty<Guid>();
            DataFilter = dataFilter;
            InTransaction = inTransaction;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            string text = Operation + " " + EntityType.Name + " [" + Label + "]: " + Strategy;
            if (Strategy == LiteGraphReadStrategy.KeyLookup) text += " (" + NodeGuids.Count + " node GUID(s))";
            else text += DataFilter != null ? " with data filter " + DataFilter : " (no data filter)";
            if (InTransaction) text += " in transaction";
            return text;
        }

        #endregion
    }
}
