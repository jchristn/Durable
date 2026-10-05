namespace Durable.InMemory
{
    using System.Threading;

    /// <summary>
    /// Generates auto-increment values for one table, like a database identity: values are never reused, rolled-back
    /// transactions leave gaps, and an explicitly stored value moves the sequence past it.
    /// Thread safety: lock-free and safe for concurrent use.
    /// </summary>
    internal sealed class InMemoryIdentitySequence
    {
        #region Private-Members

        private long _Last;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the next value.
        /// </summary>
        /// <returns>The value; the first is 1.</returns>
        public long Next()
        {
            return Interlocked.Increment(ref _Last);
        }

        /// <summary>
        /// Records an explicitly stored value so later generated values are greater.
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
