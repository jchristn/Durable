namespace Test.Shared
{
    using System.Collections.Concurrent;
    using System.Threading;

    /// <summary>
    /// A synchronization context that queues posted callbacks and never runs them, like a UI thread blocked in a
    /// synchronous call. Code that blocks on a task whose continuation was posted here deadlocks; the test bridge must
    /// therefore never let continuations capture this context.
    /// </summary>
    public sealed class NonPumpingSynchronizationContext : SynchronizationContext
    {
        #region Public-Members

        /// <summary>
        /// Gets the number of callbacks posted (and never run).
        /// </summary>
        public int PostedCount => _Posted.Count;

        #endregion

        #region Private-Members

        private readonly ConcurrentQueue<SendOrPostCallback> _Posted = new ConcurrentQueue<SendOrPostCallback>();

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override void Post(SendOrPostCallback d, object? state)
        {
            _Posted.Enqueue(d);
        }

        /// <inheritdoc />
        public override SynchronizationContext CreateCopy()
        {
            return this;
        }

        #endregion
    }
}
