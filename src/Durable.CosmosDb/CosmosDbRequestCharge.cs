namespace Durable.CosmosDb
{
    using System.Threading;

    /// <summary>
    /// Accumulates the request units charged by the Cosmos DB requests of one operation.
    /// Thread safety: safe for concurrent use.
    /// </summary>
    internal sealed class CosmosDbRequestCharge
    {
        private long _Hundredths;

        /// <summary>
        /// Gets the total request units charged so far.
        /// </summary>
        public double Total => Interlocked.Read(ref _Hundredths) / 100d;

        /// <summary>
        /// Adds the charge of one request.
        /// </summary>
        /// <param name="requestUnits">Request units of the request.</param>
        public void Add(double requestUnits)
        {
            Interlocked.Add(ref _Hundredths, (long)System.Math.Round(requestUnits * 100d));
        }
    }
}
