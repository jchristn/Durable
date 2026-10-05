namespace Durable.Query
{
    /// <summary>
    /// Aggregates over the rows of a group in an <see cref="AggregateNode"/>.
    /// </summary>
    public enum AggregateFunction
    {
        /// <summary>Number of rows (matching the predicate, when present).</summary>
        Count,
        /// <summary>Sum of the operand; zero for an empty group.</summary>
        Sum,
        /// <summary>Average of the operand.</summary>
        Average,
        /// <summary>Minimum of the operand.</summary>
        Min,
        /// <summary>Maximum of the operand.</summary>
        Max,
        /// <summary>At least one row (matching the predicate, when present).</summary>
        Any
    }
}
