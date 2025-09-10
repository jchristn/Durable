namespace Durable
{
    /// <summary>
    /// Represents a boundary of a window frame, specifying the bound type and optional offset.
    /// </summary>
    public class WindowFrameBound
    {
        /// <summary>
        /// Gets or sets the type of window frame boundary.
        /// </summary>
        public WindowFrameBoundType Type { get; set; }
        
        /// <summary>
        /// Gets or sets the optional offset value for PRECEDING or FOLLOWING bounds.
        /// </summary>
        public int? Offset { get; set; }
    }
}