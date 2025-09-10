namespace Durable
{
    /// <summary>
    /// Defines the types of window frame boundaries for window functions.
    /// </summary>
    public enum WindowFrameBoundType
    {
        /// <summary>
        /// Unbounded preceding - includes all rows from the beginning of the partition.
        /// </summary>
        UnboundedPreceding,
        
        /// <summary>
        /// Unbounded following - includes all rows to the end of the partition.
        /// </summary>
        UnboundedFollowing,
        
        /// <summary>
        /// Current row - includes only the current row.
        /// </summary>
        CurrentRow,
        
        /// <summary>
        /// Preceding - includes a specified number of rows before the current row.
        /// </summary>
        Preceding,
        
        /// <summary>
        /// Following - includes a specified number of rows after the current row.
        /// </summary>
        Following
    }
}