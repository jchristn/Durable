namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// The candidate rows read for an operation and the evaluator (with the related rows it needs) that filters, orders
    /// and computes over them.
    /// Thread safety: not thread-safe; one per operation.
    /// </summary>
    internal sealed class LiteGraphSelection
    {
        #region Public-Members

        /// <summary>
        /// Gets the candidate rows in storage order. Never null.
        /// </summary>
        public List<LiteGraphRow> Candidates { get; }

        /// <summary>
        /// Gets the evaluator. Never null.
        /// </summary>
        public LiteGraphQueryEvaluator Evaluator { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a selection.
        /// </summary>
        /// <param name="candidates">Candidate rows. Must not be null.</param>
        /// <param name="evaluator">Evaluator. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public LiteGraphSelection(List<LiteGraphRow> candidates, LiteGraphQueryEvaluator evaluator)
        {
            Candidates = candidates ?? throw new ArgumentNullException(nameof(candidates));
            Evaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
        }

        #endregion
    }
}
