namespace Durable.LiteGraph
{
    using System.Threading;

    /// <summary>
    /// Generates auto-increment values for one entity type, like a database identity: values are never reused within the
    /// process, rolled-back transactions leave gaps, and an explicitly stored value moves the sequence past it. The
    /// sequence starts after the largest key stored in the graph (read once per process).
    /// Thread safety: lock-free and safe for concurrent use.
    /// </summary>
    internal sealed class LiteGraphIdentitySequence
    {
        #region Public-Members

        /// <summary>
        /// Gets whether the sequence was initialized from storage.
        /// </summary>
        public bool Initialized => Volatile.Read(ref _Initialized) == 1;

        #endregion

        #region Private-Members

        private long _Last;
        private int _Initialized;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Marks the sequence initialized and moves it past the largest stored value.
        /// </summary>
        /// <param name="largestStored">Largest stored value (0 when none).</param>
        public void Initialize(long largestStored)
        {
            Observe(largestStored);
            Volatile.Write(ref _Initialized, 1);
        }

        /// <summary>
        /// Returns the next value.
        /// </summary>
        /// <returns>The value; the first is 1 in an empty graph.</returns>
        public long Next()
        {
            return Interlocked.Increment(ref _Last);
        }

        /// <summary>
        /// Records a stored value so later generated values are greater.
        /// </summary>
        /// <param name="value">Stored value.</param>
        public void Observe(long value)
        {
            long current = Volatile.Read(ref _Last);
            while (value > current)
            {
                long previous = Interlocked.CompareExchange(ref _Last, value, current);
                if (previous == current) return;
                current = previous;
            }
        }

        #endregion
    }
}
