namespace Durable
{
    using System;

    /// <summary>
    /// Configuration options for database connection pools.
    /// </summary>
    public class ConnectionPoolOptions
    {
        /// <summary>
        /// Gets or sets the minimum number of connections to maintain in the pool. Default is 5.
        /// </summary>
        public int MinPoolSize { get; set; } = 5;
        /// <summary>
        /// Gets or sets the maximum number of connections allowed in the pool. Default is 100.
        /// </summary>
        public int MaxPoolSize { get; set; } = 100;
        /// <summary>
        /// Gets or sets the maximum time to wait for a connection from the pool. Default is 30 seconds.
        /// </summary>
        public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);
        /// <summary>
        /// Gets or sets the maximum time a connection can remain idle before being removed. Default is 10 minutes.
        /// </summary>
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(10);
        /// <summary>
        /// Gets or sets whether connections should be validated before use. Default is true.
        /// </summary>
        public bool ValidateConnections { get; set; } = true;
    }
}