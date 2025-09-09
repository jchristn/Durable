namespace Durable
{
    using System;

    public class ConnectionPoolOptions
    {
        public int MinPoolSize { get; set; } = 5;
        public int MaxPoolSize { get; set; } = 100;
        public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(10);
        public bool ValidateConnections { get; set; } = true;
    }
}