namespace Test.Benchmark
{
    /// <summary>
    /// Convention-mapped DTO used for the raw SQL and projection benchmarks.
    /// </summary>
    public class OrderSummary
    {
        /// <summary>Order id.</summary>
        public int Id { get; set; }

        /// <summary>Customer name.</summary>
        public string CustomerName { get; set; } = string.Empty;

        /// <summary>Order total.</summary>
        public decimal Total { get; set; }

        /// <summary>Status.</summary>
        public OrderStatus Status { get; set; }
    }
}
