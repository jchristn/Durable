namespace Durable.Query
{
    /// <summary>
    /// Boolean connectives in a <see cref="LogicalNode"/>.
    /// </summary>
    public enum LogicalOperator
    {
        /// <summary>Both operands are true.</summary>
        And,
        /// <summary>Either operand is true.</summary>
        Or
    }
}
