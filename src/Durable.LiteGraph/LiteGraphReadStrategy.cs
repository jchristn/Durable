namespace Durable.LiteGraph
{
    /// <summary>
    /// How a <see cref="LiteGraphBackend"/> reads the candidate nodes of a query from LiteGraph.
    /// </summary>
    public enum LiteGraphReadStrategy
    {
        /// <summary>
        /// The filter pins every primary key column (equality, or a key <c>Contains</c> list for single-column keys), so
        /// the nodes are read by their deterministic GUIDs.
        /// </summary>
        KeyLookup,

        /// <summary>
        /// The nodes carrying the entity's label are read, narrowed by a LiteGraph data filter when part of the filter
        /// can be pushed down exactly.
        /// </summary>
        LabelScan
    }
}
