namespace Test.Benchmark
{
    /// <summary>
    /// Order status; stored as a string by Durable (the default for enums).
    /// </summary>
    public enum OrderStatus
    {
        /// <summary>Awaiting payment.</summary>
        Pending,

        /// <summary>Shipped.</summary>
        Shipped,

        /// <summary>Delivered.</summary>
        Delivered,

        /// <summary>Cancelled.</summary>
        Cancelled
    }
}
