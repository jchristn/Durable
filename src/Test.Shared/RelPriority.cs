namespace Test.Shared
{
    /// <summary>
    /// Priority stored through <see cref="PriorityCodeConverter"/> as a one-letter code.
    /// </summary>
    public enum RelPriority
    {
        /// <summary>
        /// Low priority, stored as "L".
        /// </summary>
        Low = 0,

        /// <summary>
        /// Medium priority, stored as "M".
        /// </summary>
        Medium = 1,

        /// <summary>
        /// High priority, stored as "H".
        /// </summary>
        High = 2
    }
}
