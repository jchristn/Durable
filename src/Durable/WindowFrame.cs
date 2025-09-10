namespace Durable
{
    /// <summary>
    /// Represents a window frame for window functions, defining the frame type and bounds.
    /// </summary>
    public class WindowFrame
    {
        /// <summary>
        /// Gets or sets the type of window frame (ROWS or RANGE).
        /// </summary>
        public WindowFrameType Type { get; set; }
        
        /// <summary>
        /// Gets or sets the starting bound of the window frame.
        /// </summary>
        public WindowFrameBound StartBound { get; set; } = new WindowFrameBound();
        
        /// <summary>
        /// Gets or sets the ending bound of the window frame.
        /// </summary>
        public WindowFrameBound EndBound { get; set; } = new WindowFrameBound();
    }
}