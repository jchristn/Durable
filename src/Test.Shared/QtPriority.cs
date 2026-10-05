namespace Test.Shared
{
    /// <summary>
    /// Priority used by the query translation suites. Stored as an integer (<see cref="Durable.Flags.Integer"/>).
    /// </summary>
    public enum QtPriority
    {
        /// <summary>
        /// Low priority.
        /// </summary>
        Low = 1,

        /// <summary>
        /// Medium priority.
        /// </summary>
        Medium = 2,

        /// <summary>
        /// High priority.
        /// </summary>
        High = 3,

        /// <summary>
        /// Critical priority.
        /// </summary>
        Critical = 4
    }
}
