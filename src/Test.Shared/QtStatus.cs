namespace Test.Shared
{
    /// <summary>
    /// Lifecycle status used by the query translation suites. Stored as a string (the default enum storage).
    /// </summary>
    public enum QtStatus
    {
        /// <summary>
        /// Item is a draft.
        /// </summary>
        Draft = 0,

        /// <summary>
        /// Item is active.
        /// </summary>
        Active = 1,

        /// <summary>
        /// Item is suspended.
        /// </summary>
        Suspended = 2,

        /// <summary>
        /// Item is closed.
        /// </summary>
        Closed = 3
    }
}
