namespace Durable
{
    /// <summary>
    /// Specifies the type of window frame for SQL window functions
    /// </summary>
    public enum WindowFrameType
    {
        /// <summary>
        /// Frame is defined by physical rows
        /// </summary>
        Rows,
        
        /// <summary>
        /// Frame is defined by logical range of values
        /// </summary>
        Range,
        
        /// <summary>
        /// Frame is defined by groups of equal values
        /// </summary>
        Groups
    }
}